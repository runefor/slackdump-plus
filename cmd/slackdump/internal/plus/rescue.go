package plus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rusq/slack"

	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/bootstrap"
	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/cfg"
	dedupecmd "github.com/rusq/slackdump/v4/cmd/slackdump/internal/diag/dedupe"
	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/golang/base"
	"github.com/rusq/slackdump/v4/internal/chunk"
	"github.com/rusq/slackdump/v4/internal/chunk/backend/dbase"
	"github.com/rusq/slackdump/v4/internal/client"
	"github.com/rusq/slackdump/v4/source"
)

// RescueSlackClient extends client.Slack with Pins, Stars, and Files listing methods
// promoted by the embedded *slack.Client on *client.Client.
type RescueSlackClient interface {
	client.Slack
	ListPinsContext(ctx context.Context, channel string) ([]slack.Item, *slack.Paging, error)
	ListStarsContext(ctx context.Context, params slack.StarsParameters) ([]slack.Item, string, error)
	GetFilesContext(ctx context.Context, params slack.GetFilesParameters) ([]slack.File, *slack.Paging, error)
}

var CmdRescue = &base.Command{
	UsageLine:   "slackdump rescue [flags] [archive_dir]",
	Short:       "rescue hidden (>90 days) undeleted pins, bookmarks, stars, threads, and files into the archive",
	Long: `
# Rescue Command (Deep Rescue for Free Plan 90-Day Limit)

On Slack's Free plan, messages and files older than 90 days are hidden from standard
'conversations.history' responses, even though they remain stored on Slack's servers
until the 1-year permanent deletion mark.

'slackdump rescue' scans alternative Slack API endpoints that can still surface
important items older than 90 days (or items outside normal chat history):
  1. Channel Pinned Items (pins.list) + full thread reply recovery (conversations.replies)
  2. Channel Bookmarks (bookmarks.list)
  3. Starred / Saved Items (stars.list)
  4. Workspace Standalone Files & Canvases (files.list) + forced attachment downloads
  5. Search Index Sweep (search.messages & search.files for pinned/starred/custom queries)

All rescued messages, threads, and files are merged directly into 'slackdump.sqlite'
and '__uploads/', making them immediately viewable via 'slackdump view' and 'slackdump export-md'.
`,
	RequireAuth: true,
	PrintFlags:  true,
}

type RescueOptions struct {
	Pins      bool
	Bookmarks bool
	Stars     bool
	FilesScan bool
	Search    bool
	Query     string
	MaxPages  int
	Dedupe    bool
}

var rescueOpts = RescueOptions{
	Pins:      true,
	Bookmarks: true,
	Stars:     true,
	FilesScan: true,
	Search:    true,
	Query:     "",
	MaxPages:  20,
	Dedupe:    true,
}

func init() {
	CmdRescue.Run = runRescueCmd
	CmdRescue.Flag.BoolVar(&rescueOpts.Pins, "pins", true, "rescue channel pinned messages, threads, and files (pins.list)")
	CmdRescue.Flag.BoolVar(&rescueOpts.Bookmarks, "bookmarks", true, "rescue channel bookmarks (bookmarks.list)")
	CmdRescue.Flag.BoolVar(&rescueOpts.Stars, "stars", true, "rescue starred/saved items (stars.list)")
	CmdRescue.Flag.BoolVar(&rescueOpts.FilesScan, "files-scan", true, "scan files.list for standalone files/canvases and force download")
	CmdRescue.Flag.BoolVar(&rescueOpts.Search, "search", true, "run search index sweep (has:pin, has:star) to rescue indexed items")
	CmdRescue.Flag.StringVar(&rescueOpts.Query, "query", "", "additional custom search query to rescue (e.g. 'has:link')")
	CmdRescue.Flag.IntVar(&rescueOpts.MaxPages, "max-pages", 20, "maximum pages to scan for files.list and search")
	CmdRescue.Flag.BoolVar(&rescueOpts.Dedupe, "dedupe", true, "deduplicate database records after rescue completes")
}

// RescueStats summarizes the items recovered during a rescue run.
type RescueStats struct {
	ChannelsScanned   int `json:"channels_scanned"`
	PinnedMessages    int `json:"pinned_messages"`
	PinnedThreads     int `json:"pinned_threads"`
	BookmarksRescued  int `json:"bookmarks_rescued"`
	StarredItems      int `json:"starred_items"`
	StandaloneFiles   int `json:"standalone_files"`
	SearchMessages    int `json:"search_messages"`
	FilesDownloaded   int `json:"files_downloaded"`
}

func runRescueCmd(ctx context.Context, _ *base.Command, args []string) error {
	targetDir := cfg.StripZipExt(cfg.Output)
	if len(args) > 0 && args[0] != "" {
		targetDir = cfg.StripZipExt(args[0])
	}
	if targetDir == "" {
		targetDir = "slack_backup"
	}

	cl, err := bootstrap.Slack(ctx)
	if err != nil {
		base.SetExitStatus(base.SInitializationError)
		return err
	}

	stats, err := ExecuteRescue(ctx, cl, targetDir, rescueOpts, cfg.WithFiles, cfg.Log)
	if err != nil {
		base.SetExitStatus(base.SApplicationError)
		return err
	}
	cfg.Log.InfoContext(ctx, "Deep rescue completed",
		"directory", targetDir,
		"channels_scanned", stats.ChannelsScanned,
		"pinned_msgs", stats.PinnedMessages,
		"pinned_threads", stats.PinnedThreads,
		"bookmarks", stats.BookmarksRescued,
		"starred", stats.StarredItems,
		"standalone_files", stats.StandaloneFiles,
		"search_msgs", stats.SearchMessages,
		"files_downloaded", stats.FilesDownloaded,
	)
	return nil
}

// ExecuteRescue performs the deep rescue against targetDir using the provided Slack client.
func ExecuteRescue(ctx context.Context, cl client.Slack, targetDir string, opts RescueOptions, withFiles bool, lg *slog.Logger) (RescueStats, error) {
	var stats RescueStats
	if lg == nil {
		lg = slog.Default()
	}

	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return stats, err
	}
	plusDir := filepath.Join(targetDir, "__plus")
	_ = os.MkdirAll(plusDir, 0o755)

	wconn, si, err := bootstrap.DatabaseWithSession(targetDir, "rescue")
	if err != nil {
		return stats, fmt.Errorf("open database: %w", err)
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

	// Ensure workspace info and channels are present in the DB.
	channels, err := loadOrFetchChannels(ctx, cl, dbp, targetDir, lg)
	if err != nil {
		lg.WarnContext(ctx, "unable to fetch complete channel list, continuing with available channels", "error", err)
	}

	rcl, _ := cl.(RescueSlackClient)

	// 1. Scan Pins and Bookmarks per channel
	allBookmarks := make(map[string][]slack.Bookmark)
	for _, ch := range channels {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		// Skip 1:1 IMs for bookmarks if desired, though pins can exist in any conversation.
		stats.ChannelsScanned++

		if opts.Pins && rcl != nil {
			pMsgs, pThreads, fDl := rescueChannelPins(ctx, rcl, wconn, dbp, targetDir, ch.ID, withFiles, lg)
			stats.PinnedMessages += pMsgs
			stats.PinnedThreads += pThreads
			stats.FilesDownloaded += fDl
		}

		if opts.Bookmarks && !ch.IsIM {
			bms := rescueChannelBookmarks(ctx, cl, wconn, ch.ID, lg)
			if len(bms) > 0 {
				allBookmarks[ch.ID] = bms
				stats.BookmarksRescued += len(bms)
			}
		}
	}

	if len(allBookmarks) > 0 {
		bmBytes, _ := json.MarshalIndent(allBookmarks, "", "  ")
		_ = os.WriteFile(filepath.Join(plusDir, "bookmarks.json"), bmBytes, 0o644)
	}

	// 2. Scan Starred / Saved items across the workspace
	if opts.Stars && rcl != nil {
		stCount, fDl := rescueStarredItems(ctx, rcl, wconn, dbp, targetDir, opts.MaxPages, withFiles, lg)
		stats.StarredItems += stCount
		stats.FilesDownloaded += fDl
	}

	// 3. Scan Standalone Files / Canvases via files.list
	if opts.FilesScan && rcl != nil {
		fCount, fDl := rescueWorkspaceFiles(ctx, rcl, wconn, dbp, targetDir, opts.MaxPages, withFiles, lg)
		stats.StandaloneFiles += fCount
		stats.FilesDownloaded += fDl
	}

	// 4. Search Index Sweep
	if opts.Search {
		queries := []string{"has:pin", "has:star"}
		if opts.Query != "" {
			queries = append(queries, opts.Query)
		}
		sMsgs, fDl := rescueViaSearch(ctx, cl, wconn, dbp, targetDir, queries, opts.MaxPages, withFiles, lg)
		stats.SearchMessages += sMsgs
		stats.FilesDownloaded += fDl
	}

	if err := dbp.Finish(); err != nil {
		lg.WarnContext(ctx, "warning finishing rescue session", "error", err)
	}

	// 5. Deduplicate database records
	if opts.Dedupe {
		if _, err := dedupecmd.Run(ctx, wconn, dedupecmd.Options{
			Execute:  true,
			Database: targetDir,
		}); err != nil {
			lg.WarnContext(ctx, "post-rescue dedupe warning", "error", err)
		}
	}

	// Write rescue summary report
	summaryBytes, _ := json.MarshalIndent(stats, "", "  ")
	_ = os.WriteFile(filepath.Join(plusDir, "rescue_summary.json"), summaryBytes, 0o644)

	return stats, nil
}

func loadOrFetchChannels(ctx context.Context, cl client.Slack, dbp *dbase.DBP, targetDir string, lg *slog.Logger) ([]slack.Channel, error) {
	if src, err := source.Load(ctx, targetDir); err == nil {
		defer src.Close()
		if chs, err := src.Channels(ctx); err == nil && len(chs) > 0 {
			return chs, nil
		}
	}

	// Fallback: fetch workspace info and channels directly from API.
	if wi, err := cl.AuthTestContext(ctx); err == nil && wi != nil {
		_ = dbp.Encode(ctx, &chunk.Chunk{
			Type:          chunk.CWorkspaceInfo,
			Timestamp:     time.Now().UnixNano(),
			WorkspaceInfo: wi,
		})
	}

	var all []slack.Channel
	cursor := ""
	for {
		var chs []slack.Channel
		var next string
		err := withRetry(ctx, lg, func() error {
			var e error
			chs, next, e = cl.GetConversationsContext(ctx, &slack.GetConversationsParameters{
				Cursor:          cursor,
				Limit:           200,
				Types:           []string{"public_channel", "private_channel", "mpim", "im"},
				ExcludeArchived: false,
			})
			return e
		})
		if err != nil {
			return all, err
		}
		if len(chs) > 0 {
			all = append(all, chs...)
			_ = dbp.Encode(ctx, &chunk.Chunk{
				Type:      chunk.CChannels,
				Timestamp: time.Now().UnixNano(),
				Count:     int32(len(chs)),
				Channels:  chs,
			})
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return all, nil
}

func rescueChannelPins(ctx context.Context, rcl RescueSlackClient, wconn *sqlx.DB, dbp *dbase.DBP, archiveDir, channelID string, withFiles bool, lg *slog.Logger) (pinnedMsgs, pinnedThreads, filesDl int) {
	var items []slack.Item
	err := withRetry(ctx, lg, func() error {
		var e error
		items, _, e = rcl.ListPinsContext(ctx, channelID)
		return e
	})
	if err != nil || len(items) == 0 {
		return 0, 0, 0
	}

	for _, item := range items {
		switch item.Type {
		case "message":
			if item.Message == nil {
				continue
			}
			msg := *item.Message
			dl, err := EncodeMessageWithFiles(ctx, dbp, rcl, archiveDir, channelID, msg, withFiles, lg)
			if err == nil {
				pinnedMsgs++
				filesDl += dl
			}
			_ = SaveRescuedItem(ctx, wconn, "pin", channelID, msg.Timestamp, msg.Timestamp, msg.ThreadTimestamp, "", msg)

			// If this pinned message has a thread (or is part of a thread), rescue the entire thread!
			threadTS := msg.ThreadTimestamp
			if threadTS == "" && msg.ReplyCount > 0 {
				threadTS = msg.Timestamp
			}
			if threadTS != "" {
				tCount, tDl := rescueThreadReplies(ctx, rcl, dbp, archiveDir, channelID, threadTS, &msg, withFiles, lg)
				if tCount > 0 {
					pinnedThreads++
					filesDl += tDl
				}
			}
		case "file":
			if item.File == nil {
				continue
			}
			f := *item.File
			_ = SaveRescuedItem(ctx, wconn, "pin", channelID, f.ID, item.Timestamp, "", f.ID, f)
			if withFiles {
				if ok, _, _ := ForceDownloadFile(ctx, rcl, archiveDir, &f, lg); ok {
					filesDl++
				}
			}
		}
	}
	return pinnedMsgs, pinnedThreads, filesDl
}

func rescueThreadReplies(ctx context.Context, cl client.Slack, dbp *dbase.DBP, archiveDir, channelID, threadTS string, parent *slack.Message, withFiles bool, lg *slog.Logger) (msgsRescued, filesDl int) {
	cursor := ""
	var allReplies []slack.Message
	for {
		var msgs []slack.Message
		var hasMore bool
		var next string
		err := withRetry(ctx, lg, func() error {
			var e error
			msgs, hasMore, next, e = cl.GetConversationRepliesContext(ctx, &slack.GetConversationRepliesParameters{
				ChannelID: channelID,
				Timestamp: threadTS,
				Cursor:    cursor,
				Limit:     200,
				Inclusive: true,
			})
			return e
		})
		if err != nil {
			break
		}
		allReplies = append(allReplies, msgs...)
		if !hasMore || next == "" {
			break
		}
		cursor = next
	}
	if len(allReplies) == 0 {
		return 0, 0
	}
	if parent != nil && allReplies[0].Timestamp == parent.Timestamp {
		if len(allReplies[0].Files) == 0 && len(parent.Files) > 0 {
			allReplies[0].Files = parent.Files
		}
		if len(allReplies[0].PinnedTo) == 0 && len(parent.PinnedTo) > 0 {
			allReplies[0].PinnedTo = parent.PinnedTo
		}
	}
	par := parent
	if par == nil {
		p := allReplies[0]
		par = &p
	}
	_ = dbp.Encode(ctx, &chunk.Chunk{
		Type:      chunk.CThreadMessages,
		Timestamp: time.Now().UnixNano(),
		ChannelID: channelID,
		ThreadTS:  threadTS,
		Parent:    par,
		Count:     int32(len(allReplies)),
		IsLast:    true,
		Messages:  allReplies,
	})
	for _, m := range allReplies {
		if len(m.Files) > 0 {
			p := m
			_ = dbp.Encode(ctx, &chunk.Chunk{
				Type:      chunk.CFiles,
				Timestamp: time.Now().UnixNano(),
				ChannelID: channelID,
				ThreadTS:  threadTS,
				Parent:    &p,
				Count:     int32(len(m.Files)),
				Files:     m.Files,
			})
			if withFiles {
				for i := range m.Files {
					if ok, _, _ := ForceDownloadFile(ctx, cl, archiveDir, &m.Files[i], lg); ok {
						filesDl++
					}
				}
			}
		}
	}
	return len(allReplies), filesDl
}

func rescueChannelBookmarks(ctx context.Context, cl client.Slack, wconn *sqlx.DB, channelID string, lg *slog.Logger) []slack.Bookmark {
	var bms []slack.Bookmark
	err := withRetry(ctx, lg, func() error {
		var e error
		bms, e = cl.ListBookmarks(channelID)
		return e
	})
	if err != nil || len(bms) == 0 {
		return nil
	}
	for _, bm := range bms {
		_ = SaveBookmark(ctx, wconn, channelID, bm)
	}
	return bms
}

func rescueStarredItems(ctx context.Context, rcl RescueSlackClient, wconn *sqlx.DB, dbp *dbase.DBP, archiveDir string, maxPages int, withFiles bool, lg *slog.Logger) (starredCount, filesDl int) {
	cursor := ""
	for page := 1; page <= maxPages; page++ {
		var items []slack.Item
		var next string
		err := withRetry(ctx, lg, func() error {
			var e error
			items, next, e = rcl.ListStarsContext(ctx, slack.StarsParameters{
				Cursor: cursor,
				Limit:  100,
			})
			return e
		})
		if err != nil || len(items) == 0 {
			break
		}
		for _, item := range items {
			chID := item.Channel
			if chID == "" {
				chID = "WORKSPACE"
			}
			if item.Message != nil {
				msg := *item.Message
				dl, err := EncodeMessageWithFiles(ctx, dbp, rcl, archiveDir, item.Channel, msg, withFiles, lg)
				if err == nil {
					starredCount++
					filesDl += dl
				}
				_ = SaveRescuedItem(ctx, wconn, "star", chID, msg.Timestamp, msg.Timestamp, msg.ThreadTimestamp, "", msg)
				if msg.ThreadTimestamp != "" || msg.ReplyCount > 0 {
					tTS := msg.ThreadTimestamp
					if tTS == "" {
						tTS = msg.Timestamp
					}
					_, tDl := rescueThreadReplies(ctx, rcl, dbp, archiveDir, item.Channel, tTS, &msg, withFiles, lg)
					filesDl += tDl
				}
			} else if item.File != nil {
				f := *item.File
				starredCount++
				_ = SaveRescuedItem(ctx, wconn, "star", chID, f.ID, item.Timestamp, "", f.ID, f)
				if withFiles {
					if ok, _, _ := ForceDownloadFile(ctx, rcl, archiveDir, &f, lg); ok {
						filesDl++
					}
				}
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return starredCount, filesDl
}

func rescueWorkspaceFiles(ctx context.Context, rcl RescueSlackClient, wconn *sqlx.DB, dbp *dbase.DBP, archiveDir string, maxPages int, withFiles bool, lg *slog.Logger) (filesFound, filesDl int) {
	for page := 1; page <= maxPages; page++ {
		var files []slack.File
		var paging *slack.Paging
		err := withRetry(ctx, lg, func() error {
			var e error
			files, paging, e = rcl.GetFilesContext(ctx, slack.GetFilesParameters{
				Count:      100,
				Page:       page,
				ShowHidden: true,
			})
			return e
		})
		if err != nil || len(files) == 0 {
			break
		}
		for i := range files {
			f := files[i]
			filesFound++
			chID := "WORKSPACE"
			if len(f.Channels) > 0 {
				chID = f.Channels[0]
			} else if len(f.Groups) > 0 {
				chID = f.Groups[0]
			} else if len(f.IMs) > 0 {
				chID = f.IMs[0]
			}
			ts := fmt.Sprintf("%d.000000", f.Created)
			_ = SaveRescuedItem(ctx, wconn, "file_list", chID, f.ID, ts, "", f.ID, f)
			if chID != "WORKSPACE" {
				dummyParent := slack.Message{Msg: slack.Msg{Timestamp: ts}}
				_ = dbp.Encode(ctx, &chunk.Chunk{
					Type:      chunk.CFiles,
					Timestamp: time.Now().UnixNano(),
					ChannelID: chID,
					Parent:    &dummyParent,
					Count:     1,
					Files:     []slack.File{f},
				})
			}
			if withFiles {
				if ok, _, _ := ForceDownloadFile(ctx, rcl, archiveDir, &f, lg); ok {
					filesDl++
				}
			}
		}
		if paging == nil || page >= paging.Pages {
			break
		}
	}
	return filesFound, filesDl
}

func rescueViaSearch(ctx context.Context, cl client.Slack, wconn *sqlx.DB, dbp *dbase.DBP, archiveDir string, queries []string, maxPages int, withFiles bool, lg *slog.Logger) (searchMsgs, filesDl int) {
	for _, q := range queries {
		for page := 1; page <= maxPages; page++ {
			var sm *slack.SearchMessages
			err := withRetry(ctx, lg, func() error {
				var e error
				sm, e = cl.SearchMessagesContext(ctx, q, slack.SearchParameters{
					Count: 100,
					Page:  page,
				})
				return e
			})
			if err != nil || sm == nil || len(sm.Matches) == 0 {
				break
			}
			_ = dbp.Encode(ctx, &chunk.Chunk{
				Type:           chunk.CSearchMessages,
				Timestamp:      time.Now().UnixNano(),
				SearchQuery:    q,
				Count:          int32(len(sm.Matches)),
				SearchMessages: sm.Matches,
			})
			for _, m := range sm.Matches {
				chID := m.Channel.ID
				if chID == "" {
					continue
				}
				msg := slack.Message{
					Msg: slack.Msg{
						User:        m.User,
						Username:    m.Username,
						Timestamp:   m.Timestamp,
						Text:        m.Text,
						Attachments: m.Attachments,
					},
				}
				dl, err := EncodeMessageWithFiles(ctx, dbp, cl, archiveDir, chID, msg, withFiles, lg)
				if err == nil {
					searchMsgs++
					filesDl += dl
				}
				_ = SaveRescuedItem(ctx, wconn, "search", chID, m.Timestamp, m.Timestamp, "", "", m)
			}
			if page >= sm.Paging.Pages {
				break
			}
		}

		// Also search files for the same query
		for page := 1; page <= maxPages; page++ {
			var sf *slack.SearchFiles
			err := withRetry(ctx, lg, func() error {
				var e error
				sf, e = cl.SearchFilesContext(ctx, q, slack.SearchParameters{
					Count: 100,
					Page:  page,
				})
				return e
			})
			if err != nil || sf == nil || len(sf.Matches) == 0 {
				break
			}
			_ = dbp.Encode(ctx, &chunk.Chunk{
				Type:        chunk.CSearchFiles,
				Timestamp:   time.Now().UnixNano(),
				SearchQuery: q,
				Count:       int32(len(sf.Matches)),
				SearchFiles: sf.Matches,
			})
			if withFiles {
				for i := range sf.Matches {
					if ok, _, _ := ForceDownloadFile(ctx, cl, archiveDir, &sf.Matches[i], lg); ok {
						filesDl++
					}
				}
			}
			if page >= sf.Paging.Pages {
				break
			}
		}
	}
	return searchMsgs, filesDl
}

func withRetry(ctx context.Context, lg *slog.Logger, fn func() error) error {
	const maxAttempts = 4
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		var rle *slack.RateLimitedError
		if errors.As(err, &rle) {
			wait := rle.RetryAfter
			if wait <= 0 {
				wait = 5 * time.Second
			}
			if lg != nil {
				lg.DebugContext(ctx, "rate limited by Slack, waiting", "retry_after", wait)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
				continue
			}
		}
		return err
	}
	return errors.New("max retry attempts exceeded")
}
