package plus

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rusq/slack"

	"github.com/rusq/slackdump/v4/internal/chunk"
	"github.com/rusq/slackdump/v4/internal/chunk/backend/dbase"
	"github.com/rusq/slackdump/v4/internal/client"
	"github.com/rusq/slackdump/v4/source"
)

const plusSchemaSQL = `
CREATE TABLE IF NOT EXISTS plus_rescued_items (
	source_type TEXT NOT NULL,
	channel_id  TEXT NOT NULL,
	item_id     TEXT NOT NULL,
	ts          TEXT,
	thread_ts   TEXT,
	file_id     TEXT,
	raw_json    TEXT NOT NULL,
	rescued_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (source_type, channel_id, item_id)
);

CREATE TABLE IF NOT EXISTS plus_bookmarks (
	id           TEXT NOT NULL,
	channel_id   TEXT NOT NULL,
	title        TEXT,
	link         TEXT,
	emoji        TEXT,
	type         TEXT,
	date_created INTEGER,
	date_updated INTEGER,
	raw_json     TEXT NOT NULL,
	rescued_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (channel_id, id)
);
`

// InitPlusSchema creates the extension tables in the SQLite database if they do not exist.
func InitPlusSchema(ctx context.Context, conn *sqlx.DB) error {
	_, err := conn.ExecContext(ctx, plusSchemaSQL)
	if err != nil {
		return fmt.Errorf("init plus schema: %w", err)
	}
	return nil
}

// SaveRescuedItem records a rescued item into plus_rescued_items.
func SaveRescuedItem(ctx context.Context, conn *sqlx.DB, sourceType, channelID, itemID, ts, threadTS, fileID string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	const q = `
INSERT INTO plus_rescued_items (source_type, channel_id, item_id, ts, thread_ts, file_id, raw_json, rescued_at)
VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(source_type, channel_id, item_id) DO UPDATE SET
	raw_json = excluded.raw_json,
	rescued_at = CURRENT_TIMESTAMP;
`
	_, err = conn.ExecContext(ctx, q, sourceType, channelID, itemID, ts, threadTS, fileID, string(raw))
	return err
}

// SaveBookmark records a channel bookmark into plus_bookmarks.
func SaveBookmark(ctx context.Context, conn *sqlx.DB, channelID string, bm slack.Bookmark) error {
	raw, err := json.Marshal(bm)
	if err != nil {
		return err
	}
	const q = `
INSERT INTO plus_bookmarks (id, channel_id, title, link, emoji, type, date_created, date_updated, raw_json, rescued_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(channel_id, id) DO UPDATE SET
	title = excluded.title,
	link = excluded.link,
	emoji = excluded.emoji,
	type = excluded.type,
	date_updated = excluded.date_updated,
	raw_json = excluded.raw_json,
	rescued_at = CURRENT_TIMESTAMP;
`
	_, err = conn.ExecContext(ctx, q, bm.ID, channelID, bm.Title, bm.Link, bm.Emoji, bm.Type, int64(bm.Created), int64(bm.Updated), string(raw))
	return err
}

// StoredBookmark represents a row in plus_bookmarks.
type StoredBookmark struct {
	ID          string `db:"id" json:"id"`
	ChannelID   string `db:"channel_id" json:"channel_id"`
	Title       string `db:"title" json:"title"`
	Link        string `db:"link" json:"link"`
	Emoji       string `db:"emoji" json:"emoji"`
	Type        string `db:"type" json:"type"`
	DateCreated int64  `db:"date_created" json:"date_created"`
	DateUpdated int64  `db:"date_updated" json:"date_updated"`
	RawJSON     string `db:"raw_json" json:"-"`
}

// LoadBookmarksByChannel returns all rescued bookmarks grouped by channel_id.
func LoadBookmarksByChannel(ctx context.Context, conn *sqlx.DB) (map[string][]StoredBookmark, error) {
	result := make(map[string][]StoredBookmark)
	if conn == nil {
		return result, nil
	}
	if err := InitPlusSchema(ctx, conn); err != nil {
		return result, nil
	}
	var rows []StoredBookmark
	err := conn.SelectContext(ctx, &rows, `SELECT id, channel_id, title, link, emoji, type, date_created, date_updated, raw_json FROM plus_bookmarks ORDER BY date_created ASC`)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, nil
		}
		return result, err
	}
	for _, r := range rows {
		result[r.ChannelID] = append(result[r.ChannelID], r)
	}
	return result, nil
}

// StoredPin represents a rescued pinned message or file in plus_rescued_items.
type StoredPin struct {
	SourceType string `db:"source_type"`
	ChannelID  string `db:"channel_id"`
	ItemID     string `db:"item_id"`
	TS         string `db:"ts"`
	ThreadTS   string `db:"thread_ts"`
	FileID     string `db:"file_id"`
	RawJSON    string `db:"raw_json"`
}

// LoadRescuedPinsByChannel returns rescued pin/star items grouped by channel_id.
func LoadRescuedPinsByChannel(ctx context.Context, conn *sqlx.DB) (map[string][]StoredPin, error) {
	result := make(map[string][]StoredPin)
	if conn == nil {
		return result, nil
	}
	if err := InitPlusSchema(ctx, conn); err != nil {
		return result, nil
	}
	var rows []StoredPin
	err := conn.SelectContext(ctx, &rows, `SELECT source_type, channel_id, item_id, COALESCE(ts, '') AS ts, COALESCE(thread_ts, '') AS thread_ts, COALESCE(file_id, '') AS file_id, raw_json FROM plus_rescued_items WHERE source_type IN ('pin', 'star') ORDER BY ts ASC`)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, nil
		}
		return result, err
	}
	for _, r := range rows {
		result[r.ChannelID] = append(result[r.ChannelID], r)
	}
	return result, nil
}

// FirstDownloadURL returns the best available download URL from a slack.File,
// even if the file mode is marked "hidden_by_limit" on a free workspace.
func FirstDownloadURL(f *slack.File) string {
	if f == nil {
		return ""
	}
	for _, u := range []string{
		f.URLPrivateDownload,
		f.URLPrivate,
		f.URLDownload,
		f.PermalinkPublic,
	} {
		if strings.TrimSpace(u) != "" && strings.HasPrefix(u, "http") {
			return u
		}
	}
	return ""
}

// ForceDownloadFile attempts to download a Slack file to <archiveDir>/__uploads/<fileID>/<sanitizedName>
// even if the file is older than 90 days (as long as the file has not yet been purged by Slack's 1-year deletion).
// It skips downloading if the target file already exists and is non-empty.
func ForceDownloadFile(ctx context.Context, cl client.Slack, archiveDir string, f *slack.File, lg *slog.Logger) (downloaded bool, relPath string, err error) {
	if f == nil || f.ID == "" {
		return false, "", nil
	}
	name := f.Name
	if name == "" {
		name = f.Title
	}
	if name == "" {
		name = f.ID
	}
	relPath = source.MattermostFilepath(nil, &slack.File{ID: f.ID, Name: name})
	fullPath := filepath.Join(archiveDir, relPath)

	if fi, statErr := os.Stat(fullPath); statErr == nil && fi.Size() > 0 {
		return false, relPath, nil
	}

	dlURL := FirstDownloadURL(f)
	if dlURL == "" && cl != nil {
		// Attempt to query files.info in case the full File object still holds the URL on the backend.
		if fullFile, _, _, infoErr := cl.GetFileInfoContext(ctx, f.ID, 0, 1); infoErr == nil && fullFile != nil {
			*f = *fullFile
			dlURL = FirstDownloadURL(f)
		}
	}
	if dlURL == "" {
		return false, relPath, nil
	}

	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return false, relPath, err
	}

	tmpPath := fullPath + ".part"
	outFile, err := os.Create(tmpPath)
	if err != nil {
		return false, relPath, err
	}

	var dlErr error
	// If the URL has an embedded export token (?t=xoxe-...), try direct HTTP download first
	// or fall back to the authenticated Slack client.
	if strings.Contains(dlURL, "?t=") || cl == nil {
		dlErr = downloadDirectHTTP(ctx, dlURL, outFile)
		if dlErr != nil && cl != nil {
			_, _ = outFile.Seek(0, 0)
			_ = outFile.Truncate(0)
			dlErr = cl.GetFileContext(ctx, dlURL, outFile)
		}
	} else {
		dlErr = cl.GetFileContext(ctx, dlURL, outFile)
	}

	closeErr := outFile.Close()
	if dlErr != nil {
		_ = os.Remove(tmpPath)
		if lg != nil {
			lg.DebugContext(ctx, "unable to download file (may be expired or hidden)", "file_id", f.ID, "name", f.Name, "error", dlErr)
		}
		return false, relPath, dlErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return false, relPath, closeErr
	}

	// Verify we did not just download an HTML login page by mistake when a file is hidden_by_limit.
	if isLikelyHTMLLoginPage(tmpPath, f.Mimetype) {
		_ = os.Remove(tmpPath)
		return false, relPath, errors.New("server returned HTML login/redirect instead of file payload (hidden by 90-day limit)")
	}

	if err := os.Rename(tmpPath, fullPath); err != nil {
		_ = os.Remove(tmpPath)
		return false, relPath, err
	}
	return true, relPath, nil
}

func downloadDirectHTTP(ctx context.Context, url string, w io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	hc := &http.Client{Timeout: 60 * time.Second}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP status %d", resp.StatusCode)
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

func isLikelyHTMLLoginPage(path string, expectedMime string) bool {
	if strings.Contains(strings.ToLower(expectedMime), "html") {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	buf := make([]byte, 512)
	n, _ := f.Read(buf)
	if n == 0 {
		return true
	}
	head := strings.ToLower(strings.TrimSpace(string(buf[:n])))
	return strings.HasPrefix(head, "<!doctype html") || strings.HasPrefix(head, "<html")
}

// EncodeMessageWithFiles inserts a message (and its files chunk) into DBP and downloads attachments.
func EncodeMessageWithFiles(ctx context.Context, dbp *dbase.DBP, cl client.Slack, archiveDir, channelID string, msg slack.Message, withFiles bool, lg *slog.Logger) (filesDownloaded int, err error) {
	if channelID == "" {
		return 0, nil
	}
	now := time.Now().UnixNano()
	msgChunk := &chunk.Chunk{
		Type:      chunk.CMessages,
		Timestamp: now,
		ChannelID: channelID,
		Count:     1,
		Messages:  []slack.Message{msg},
	}
	if err := dbp.Encode(ctx, msgChunk); err != nil {
		return 0, err
	}
	if len(msg.Files) > 0 {
		parent := msg
		fileChunk := &chunk.Chunk{
			Type:      chunk.CFiles,
			Timestamp: now,
			ChannelID: channelID,
			ThreadTS:  msg.ThreadTimestamp,
			Parent:    &parent,
			Count:     int32(len(msg.Files)),
			Files:     msg.Files,
		}
		_ = dbp.Encode(ctx, fileChunk)
		if withFiles {
			for i := range msg.Files {
				ok, _, _ := ForceDownloadFile(ctx, cl, archiveDir, &msg.Files[i], lg)
				if ok {
					filesDownloaded++
				}
			}
		}
	}
	return filesDownloaded, nil
}

// IterateChannelMessages safely iterates over all channel messages and thread replies
// for any source.Sourcer implementation (including source.Export where Sorted is unimplemented).
func IterateChannelMessages(ctx context.Context, src source.Sourcer, channelID string, cb func(msg *slack.Message) error) error {
	if !src.Type().Has(source.FExport) {
		return src.Sorted(ctx, channelID, false, func(_ time.Time, msg *slack.Message) error {
			return cb(msg)
		})
	}

	it, err := src.AllMessages(ctx, channelID)
	if err != nil {
		return err
	}
	seenThreads := make(map[string]bool)
	for m, err := range it {
		if err != nil {
			return err
		}
		msgCopy := m
		if err := cb(&msgCopy); err != nil {
			return err
		}
		threadTS := m.ThreadTimestamp
		if threadTS == "" && m.ReplyCount > 0 {
			threadTS = m.Timestamp
		}
		if threadTS != "" && !seenThreads[threadTS] {
			seenThreads[threadTS] = true
			tit, terr := src.AllThreadMessages(ctx, channelID, threadTS)
			if terr != nil {
				continue
			}
			for tm, terr := range tit {
				if terr != nil {
					break
				}
				// Skip the parent message since it was already emitted by AllMessages
				if tm.Timestamp == m.Timestamp {
					continue
				}
				tmCopy := tm
				if err := cb(&tmCopy); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

