package plus

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rusq/fsadapter"

	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/archive"
	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/bootstrap"
	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/cfg"
	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/emoji/emojidl"
	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/golang/base"
	"github.com/rusq/slackdump/v4/internal/chunk/backend/dbase"
	"github.com/rusq/slackdump/v4/internal/chunk/backend/dbase/repository"
	"github.com/rusq/slackdump/v4/internal/chunk/control"
	"github.com/rusq/slackdump/v4/internal/client"
	"github.com/rusq/slackdump/v4/internal/structures"
	"github.com/rusq/slackdump/v4/source"
	"github.com/rusq/slackdump/v4/stream"
)

var CmdFullBackup = &base.Command{
	UsageLine: "slackdump full-backup [flags] [channel_links...]",
	Short:     "one-click full workspace backup (all channels, DMs, threads, files, emojis + >90-day rescue + Markdown)",
	Long: `
# Full-Backup Command (All-in-One Workspace Archiver & >90-Day Rescuer)

Runs a comprehensive, multi-stage backup into a single persistent directory
(defaults to './slack_backup' if '-o' is not specified):

  Stage 1 [Full Archive]:
    Downloads all users, avatars, public/private channels, group DMs, 1:1 DMs,
    messages, thread replies, and file attachments into 'slackdump.sqlite' and '__uploads/'.
    Automatically deduplicates files across repeated runs.

  Stage 2 [Custom Emojis]:
    Downloads all workspace custom emojis into '__emojis/'.

  Stage 3 [Deep Rescue (>90-Day Undeleted Data)]:
    Scans pins.list, bookmarks.list, stars.list, files.list (including hidden files),
    and search indexes to rescue older messages, threads, and attachments that are
    hidden from normal history on the Free plan but not yet deleted (under 1 year).

  Stage 4 [Optional Official Export ZIP Merge (-import-zip)]:
    If '-import-zip <path>' is provided, merges an official Slack Export ZIP and
    force-downloads undeleted historical file attachments.

  Stage 5 [Markdown Knowledge Base]:
    Generates human-readable Markdown logs ('markdown/INDEX.md' & per-channel logs)
    with inline links to locally downloaded images and files.
`,
	FlagMask:    cfg.OmitUserCacheFlag | cfg.OmitChunkFileMode,
	RequireAuth: true,
	PrintFlags:  true,
}

type FullBackupOptions struct {
	WithEmojis   bool
	WithRescue   bool
	WithMarkdown bool
	ImportZip    string
}

var fullBackupOpts = FullBackupOptions{
	WithEmojis:   true,
	WithRescue:   true,
	WithMarkdown: true,
}

func init() {
	CmdFullBackup.Run = runFullBackupCmd
	CmdFullBackup.Flag.BoolVar(&fullBackupOpts.WithEmojis, "emojis", true, "download all custom workspace emojis into __emojis/")
	CmdFullBackup.Flag.BoolVar(&fullBackupOpts.WithRescue, "rescue", true, "run deep rescue (pins, bookmarks, stars, standalone files, search) for >90-day data")
	CmdFullBackup.Flag.BoolVar(&fullBackupOpts.WithMarkdown, "md", true, "generate readable Markdown archive under <output>/markdown")
	CmdFullBackup.Flag.StringVar(&fullBackupOpts.ImportZip, "import-zip", "", "optional path to an official Slack Export .zip/folder to merge and rescue >90-day files from")
}

func resolveFullBackupDir(rawOutput string) string {
	// If the user didn't customize -o (it still has the auto-generated slackdump_YYYYMMDD_HHMMSS.zip default),
	// use a deterministic persistent folder './slack_backup' so subsequent runs accumulate cleanly.
	if strings.HasPrefix(rawOutput, "slackdump_") && strings.HasSuffix(rawOutput, ".zip") {
		return "slack_backup"
	}
	dir := cfg.StripZipExt(rawOutput)
	if dir == "" {
		return "slack_backup"
	}
	return dir
}

func runFullBackupCmd(ctx context.Context, cmd *base.Command, args []string) error {
	start := time.Now()
	lg := cfg.Log

	targetDir := resolveFullBackupDir(cfg.Output)
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return err
	}

	// Ensure files and records are enabled by default for full-backup.
	cfg.RecordFiles = true
	if !isFlagExplicitlySet(cmd, "avatars") {
		cfg.WithAvatars = true
	}

	list, err := structures.NewEntityList(args)
	if err != nil {
		base.SetExitStatus(base.SUserError)
		return err
	}

	cl, err := bootstrap.Slack(ctx)
	if err != nil {
		base.SetExitStatus(base.SInitializationError)
		return err
	}

	// Stage 1: Full / Incremental Database Archive
	lg.InfoContext(ctx, "[1/5] Starting full workspace conversation, thread & file archive...", "directory", targetDir)
	if err := runStageArchive(ctx, cmd.Name(), cl, targetDir, list); err != nil {
		base.SetExitStatus(base.SApplicationError)
		return fmt.Errorf("stage 1 (archive) failed: %w", err)
	}

	// Stage 2: Custom Emojis
	if fullBackupOpts.WithEmojis {
		emojiDir := filepath.Join(targetDir, "__emojis")
		lg.InfoContext(ctx, "[2/5] Downloading workspace custom emojis...", "directory", emojiDir)
		if err := os.MkdirAll(emojiDir, 0o755); err == nil {
			efsa := fsadapter.NewDirectory(emojiDir)
			err := emojidl.DlFS(ctx, cl, efsa, &emojidl.Options{
				WithFiles: cfg.WithFiles,
				FailFast:  false,
			}, nil)
			_ = efsa.Close()
			if err != nil {
				lg.WarnContext(ctx, "custom emoji download had non-fatal warnings", "error", err)
			}
		}
	}

	// Stage 3: Deep Rescue (>90-Day Undeleted Pins, Bookmarks, Stars, Files, Search)
	if fullBackupOpts.WithRescue {
		lg.InfoContext(ctx, "[3/5] Running Deep Rescue for >90-day undeleted pins, bookmarks, stars, threads, and files...")
		rStats, err := ExecuteRescue(ctx, cl, targetDir, rescueOpts, cfg.WithFiles, lg)
		if err != nil {
			lg.WarnContext(ctx, "deep rescue stage encountered warnings", "error", err)
		} else {
			lg.InfoContext(ctx, "Deep rescue summary",
				"pinned_msgs", rStats.PinnedMessages,
				"pinned_threads", rStats.PinnedThreads,
				"bookmarks", rStats.BookmarksRescued,
				"starred", rStats.StarredItems,
				"standalone_files", rStats.StandaloneFiles,
				"files_downloaded", rStats.FilesDownloaded,
			)
		}
	}

	// Stage 4: Optional Official Export ZIP Merge
	if fullBackupOpts.ImportZip != "" {
		lg.InfoContext(ctx, "[4/5] Merging official Slack Export ZIP & rescuing historical files...", "zip", fullBackupOpts.ImportZip)
		zStats, err := ExecuteRescueZip(ctx, cl, fullBackupOpts.ImportZip, targetDir, cfg.WithFiles, true, lg)
		if err != nil {
			lg.WarnContext(ctx, "import-zip stage encountered warnings", "error", err)
		} else {
			lg.InfoContext(ctx, "Official export merge summary",
				"channels", zStats.ChannelsMerged,
				"messages", zStats.MessagesMerged,
				"threads", zStats.ThreadsMerged,
				"files_downloaded", zStats.FilesDownloaded,
			)
		}
	}

	// Stage 5: Markdown Knowledge Base Generation
	if fullBackupOpts.WithMarkdown {
		mdDir := filepath.Join(targetDir, "markdown")
		lg.InfoContext(ctx, "[5/5] Generating readable Markdown archive...", "markdown_dir", mdDir)
		mStats, err := ExecuteExportMD(ctx, targetDir, mdDir, lg)
		if err != nil {
			lg.WarnContext(ctx, "markdown generation encountered warnings", "error", err)
		} else {
			lg.InfoContext(ctx, "Markdown generation completed",
				"channels", mStats.ChannelsExported,
				"messages", mStats.MessagesExported,
				"md_files", mStats.MarkdownFiles,
			)
		}
	}

	lg.InfoContext(ctx, "Full backup completed successfully!",
		"directory", targetDir,
		"sqlite_db", filepath.Join(targetDir, source.DefaultDBFile),
		"markdown_index", filepath.Join(targetDir, "markdown", "INDEX.md"),
		"took", time.Since(start).Round(time.Millisecond),
	)
	return nil
}

func runStageArchive(ctx context.Context, cmdName string, sClient client.Slack, targetDir string, list *structures.EntityList) error {
	dbfile := filepath.Join(targetDir, source.DefaultDBFile)
	conn, err := sqlx.Open(repository.Driver, dbfile)
	if err != nil {
		return err
	}
	defer conn.Close()

	flags := control.Flags{
		MemberOnly:    cfg.MemberOnly,
		RecordFiles:   true,
		ChannelUsers:  cfg.OnlyChannelUsers,
		IncludeLabels: cfg.IncludeCustomLabels,
		ChannelTypes:  cfg.ChannelTypes,
		Refresh:       true,
	}

	streamOpts := []stream.Option{
		stream.OptIncludeOlderCanvasRoots(),
	}

	ctrl, err := archive.DBController(
		ctx,
		cmdName,
		conn,
		sClient,
		targetDir,
		flags,
		streamOpts,
		archive.WithFileDeduplication(),
		archive.WithDatabaseOptions(
			dbase.WithOnlyNewOrChangedUsers(true),
		),
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := ctrl.Close(); err != nil {
			slog.ErrorContext(ctx, "unable to close database controller", "error", err)
		}
	}()

	if err := ctrl.RunNoTransform(ctx, list); err != nil {
		return err
	}
	return ctrl.Finish()
}

func isFlagExplicitlySet(cmd *base.Command, name string) bool {
	found := false
	cmd.Flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
