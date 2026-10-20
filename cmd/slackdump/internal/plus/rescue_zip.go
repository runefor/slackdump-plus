package plus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/rusq/slack"

	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/bootstrap"
	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/cfg"
	dedupecmd "github.com/rusq/slackdump/v4/cmd/slackdump/internal/diag/dedupe"
	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/golang/base"
	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/workspace"
	"github.com/rusq/slackdump/v4/internal/chunk"
	"github.com/rusq/slackdump/v4/internal/chunk/backend/dbase"
	"github.com/rusq/slackdump/v4/internal/client"
	"github.com/rusq/slackdump/v4/source"
)

var CmdRescueZip = &base.Command{
	UsageLine: "slackdump rescue-zip [flags] <official-export.zip_or_dir>",
	Short:     "merge an official Slack export ZIP/folder (>90 days history) into SQLite and rescue undeleted files",
	Long: `
# Rescue-Zip Command (Official Export Merger & Hidden File Downloader)

On Slack's Free plan, Workspace Owners/Admins can still request a Standard Export (.zip)
from Slack's web settings. That export often contains public channel messages older than
90 days (between day 91 and day 365, before Slack's 1-year permanent deletion), along
with private file download URLs ('url_private_download').

'slackdump rescue-zip' reads an official Slack Export (.zip or directory):
  1. Merges all users, channels, messages, and thread replies into '<output>/slackdump.sqlite'
  2. Extracts every attachment URL from historical messages (including >90-day messages)
     and forces local download into '<output>/__uploads/<file_id>/<filename>' if the file
     has not yet been purged from Slack's storage servers
  3. Runs database deduplication so overlapping history is cleanly unified
`,
	RequireAuth: false,
	PrintFlags:  true,
}

type RescueZipOptions struct {
	WithAuth bool
	Dedupe   bool
}

var rescueZipOpts = RescueZipOptions{
	WithAuth: false,
	Dedupe:   true,
}

func init() {
	CmdRescueZip.Run = runRescueZipCmd
	CmdRescueZip.Flag.BoolVar(&rescueZipOpts.WithAuth, "use-auth", false, "also initialize authenticated Slack session to download files lacking embedded export tokens")
	CmdRescueZip.Flag.BoolVar(&rescueZipOpts.Dedupe, "dedupe", true, "deduplicate database records after merging the export")
}

// ZipRescueStats tracks statistics for a rescue-zip operation.
type ZipRescueStats struct {
	ChannelsMerged  int `json:"channels_merged"`
	UsersMerged     int `json:"users_merged"`
	MessagesMerged  int `json:"messages_merged"`
	ThreadsMerged   int `json:"threads_merged"`
	FilesDownloaded int `json:"files_downloaded"`
	FilesCopied     int `json:"files_copied"`
}

func runRescueZipCmd(ctx context.Context, _ *base.Command, args []string) error {
	if len(args) < 1 || args[0] == "" {
		base.SetExitStatus(base.SInvalidParameters)
		return errors.New("path to official Slack export (.zip or directory) is required")
	}
	exportSrc := args[0]
	targetDir := cfg.StripZipExt(cfg.Output)
	if len(args) > 1 && args[1] != "" {
		targetDir = cfg.StripZipExt(args[1])
	}
	if targetDir == "" {
		targetDir = "slack_backup"
	}

	var cl client.Slack
	if rescueZipOpts.WithAuth {
		if authCtx, err := workspace.CurrentOrNewProviderCtx(ctx); err == nil {
			ctx = authCtx
			if c, err := bootstrap.Slack(ctx); err == nil {
				cl = c
			}
		}
	}

	stats, err := ExecuteRescueZip(ctx, cl, exportSrc, targetDir, cfg.WithFiles, rescueZipOpts.Dedupe, cfg.Log)
	if err != nil {
		base.SetExitStatus(base.SApplicationError)
		return err
	}

	cfg.Log.InfoContext(ctx, "Export ZIP rescue & merge completed",
		"export_source", exportSrc,
		"target_archive", targetDir,
		"channels", stats.ChannelsMerged,
		"users", stats.UsersMerged,
		"messages", stats.MessagesMerged,
		"threads", stats.ThreadsMerged,
		"files_downloaded", stats.FilesDownloaded,
		"files_copied", stats.FilesCopied,
	)
	return nil
}

// ExecuteRescueZip merges an export source into targetDir's SQLite database and rescues attachments.
func ExecuteRescueZip(ctx context.Context, cl client.Slack, exportSrc, targetDir string, downloadFiles, dedupe bool, lg *slog.Logger) (ZipRescueStats, error) {
	var stats ZipRescueStats
	if lg == nil {
		lg = slog.Default()
	}

	src, err := source.Load(ctx, exportSrc)
	if err != nil {
		return stats, fmt.Errorf("load export source %q: %w", exportSrc, err)
	}
	defer src.Close()

	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return stats, err
	}

	wconn, si, err := bootstrap.DatabaseWithSession(targetDir, "rescue-zip")
	if err != nil {
		return stats, fmt.Errorf("open target database: %w", err)
	}
	defer wconn.Close()

	dbp, err := dbase.New(ctx, wconn, si, dbase.WithOnlyNewOrChangedUsers(true))
	if err != nil {
		return stats, fmt.Errorf("init db processor: %w", err)
	}
	defer dbp.Close()

	if err := InitPlusSchema(ctx, wconn); err != nil {
		return stats, err
	}

	// 1. Merge WorkspaceInfo if present
	if wi, err := src.WorkspaceInfo(ctx); err == nil && wi != nil {
		_ = dbp.Encode(ctx, &chunk.Chunk{
			Type:          chunk.CWorkspaceInfo,
			Timestamp:     time.Now().UnixNano(),
			WorkspaceInfo: wi,
		})
	}

	// 2. Merge Users
	if users, err := src.Users(ctx); err == nil && len(users) > 0 {
		if err := dbp.Encode(ctx, &chunk.Chunk{
			Type:      chunk.CUsers,
			Timestamp: time.Now().UnixNano(),
			Count:     int32(len(users)),
			Users:     users,
		}); err == nil {
			stats.UsersMerged = len(users)
		}
	}

	// 3. Merge Channels & Messages & Threads & Files
	channels, err := src.Channels(ctx)
	if err != nil && !errors.Is(err, source.ErrNotFound) {
		return stats, fmt.Errorf("read channels from export: %w", err)
	}
	if len(channels) > 0 {
		_ = dbp.Encode(ctx, &chunk.Chunk{
			Type:      chunk.CChannels,
			Timestamp: time.Now().UnixNano(),
			Count:     int32(len(channels)),
			Channels:  channels,
		})
	}

	for _, ch := range channels {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		chCopy := ch
		_ = dbp.Encode(ctx, &chunk.Chunk{
			Type:      chunk.CChannelInfo,
			Timestamp: time.Now().UnixNano(),
			ChannelID: ch.ID,
			Channel:   &chCopy,
		})

		var chanMsgs []slack.Message
		threads := make(map[string][]slack.Message)
		threadParents := make(map[string]slack.Message)

		err := IterateChannelMessages(ctx, src, ch.ID, func(msg *slack.Message) error {
			if msg == nil {
				return nil
			}
			m := *msg
			// If it is a thread reply (not the thread lead), group it into threads[thread_ts]
			if m.ThreadTimestamp != "" && m.ThreadTimestamp != m.Timestamp {
				threads[m.ThreadTimestamp] = append(threads[m.ThreadTimestamp], m)
			} else {
				chanMsgs = append(chanMsgs, m)
				if m.ThreadTimestamp != "" && m.ThreadTimestamp == m.Timestamp {
					threadParents[m.ThreadTimestamp] = m
					threads[m.ThreadTimestamp] = append([]slack.Message{m}, threads[m.ThreadTimestamp]...)
				}
			}

			// Rescue files referenced in this message
			if len(m.Files) > 0 {
				parent := m
				_ = dbp.Encode(ctx, &chunk.Chunk{
					Type:      chunk.CFiles,
					Timestamp: time.Now().UnixNano(),
					ChannelID: ch.ID,
					ThreadTS:  m.ThreadTimestamp,
					Parent:    &parent,
					Count:     int32(len(m.Files)),
					Files:     m.Files,
				})
				for i := range m.Files {
					f := &m.Files[i]
					// First check if the export archive itself already contains this local file
					if copied := copyFileFromSourceStorage(src.Files(), targetDir, f); copied {
						stats.FilesCopied++
						continue
					}
					if downloadFiles {
						if ok, _, _ := ForceDownloadFile(ctx, cl, targetDir, f, lg); ok {
							stats.FilesDownloaded++
						}
					}
				}
			}
			return nil
		})
		if err != nil && !errors.Is(err, source.ErrNotFound) {
			lg.WarnContext(ctx, "warning reading channel messages from export", "channel", ch.ID, "error", err)
		}

		if len(chanMsgs) > 0 {
			stats.ChannelsMerged++
			stats.MessagesMerged += len(chanMsgs)
			_ = dbp.Encode(ctx, &chunk.Chunk{
				Type:      chunk.CMessages,
				Timestamp: time.Now().UnixNano(),
				ChannelID: ch.ID,
				Count:     int32(len(chanMsgs)),
				IsLast:    true,
				Messages:  chanMsgs,
			})
		}

		for tTS, tMsgs := range threads {
			if len(tMsgs) == 0 {
				continue
			}
			par, ok := threadParents[tTS]
			if !ok {
				par = tMsgs[0]
			}
			stats.ThreadsMerged++
			_ = dbp.Encode(ctx, &chunk.Chunk{
				Type:      chunk.CThreadMessages,
				Timestamp: time.Now().UnixNano(),
				ChannelID: ch.ID,
				ThreadTS:  tTS,
				Parent:    &par,
				Count:     int32(len(tMsgs)),
				IsLast:    true,
				Messages:  tMsgs,
			})
		}
	}

	if err := dbp.Finish(); err != nil {
		lg.WarnContext(ctx, "warning finishing rescue-zip session", "error", err)
	}

	if dedupe {
		if _, err := dedupecmd.Run(ctx, wconn, dedupecmd.Options{
			Execute:  true,
			Database: targetDir,
		}); err != nil {
			lg.WarnContext(ctx, "post-merge dedupe warning", "error", err)
		}
	}

	return stats, nil
}

func copyFileFromSourceStorage(st source.Storage, targetDir string, f *slack.File) bool {
	if st == nil || st.Type() == source.STnone || f == nil || f.ID == "" {
		return false
	}
	name := f.Name
	if name == "" {
		name = f.Title
	}
	if name == "" {
		name = f.ID
	}
	srcRel, err := st.File(f.ID, name)
	if err != nil {
		return false
	}
	dstRel := source.MattermostFilepath(nil, &slack.File{ID: f.ID, Name: name})
	dstFull := filepath.Join(targetDir, dstRel)
	if fi, err := os.Stat(dstFull); err == nil && fi.Size() > 0 {
		return false
	}
	inFile, err := fs.FS(st.FS()).Open(srcRel)
	if err != nil {
		return false
	}
	defer inFile.Close()

	if err := os.MkdirAll(filepath.Dir(dstFull), 0o755); err != nil {
		return false
	}
	outFile, err := os.Create(dstFull)
	if err != nil {
		return false
	}
	defer outFile.Close()
	n, err := io.Copy(outFile, inFile)
	return err == nil && n > 0
}
