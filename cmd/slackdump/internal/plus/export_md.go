package plus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/rusq/slack"

	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/bootstrap"
	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/cfg"
	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/golang/base"
	"github.com/rusq/slackdump/v4/internal/fasttime"
	"github.com/rusq/slackdump/v4/source"
)

var CmdExportMD = &base.Command{
	UsageLine: "slackdump export-md [flags] [archive_dir_or_sqlite]",
	Short:     "export SQLite/archive data into Obsidian & Git-friendly Markdown files with local image links",
	Long: `
# Export-MD Command (Markdown Knowledge Base Generator)

Converts a slackdump archive (SQLite database directory, .sqlite file, or export ZIP)
into a clean, human-readable hierarchy of Markdown (.md) documents:
  - INDEX.md: Workspace overview, statistics, and channel directory links
  - channels/<channel_name>/README.md: Channel metadata, rescued Bookmarks, and Pinned messages
  - channels/<channel_name>/<YYYY-MM>.md: Monthly chat logs with thread replies nested under
    parent messages, user/channel mentions resolved to real names, and inline relative links
    to locally downloaded images and attachments in '__uploads/'.
`,
	RequireAuth: false,
	PrintFlags:  true,
}

var mdOutFlag string

func init() {
	CmdExportMD.Run = runExportMDCmd
	CmdExportMD.Flag.StringVar(&mdOutFlag, "md-out", "", "target directory for generated Markdown files (defaults to <archive_dir>/markdown)")
}

// MDExportStats holds counts of generated Markdown artifacts.
type MDExportStats struct {
	ChannelsExported int `json:"channels_exported"`
	MessagesExported int `json:"messages_exported"`
	ThreadsExported  int `json:"threads_exported"`
	FilesLinked      int `json:"files_linked"`
	MarkdownFiles    int `json:"markdown_files"`
}

func runExportMDCmd(ctx context.Context, _ *base.Command, args []string) error {
	srcPath := cfg.StripZipExt(cfg.Output)
	if len(args) > 0 && args[0] != "" {
		srcPath = args[0]
	}
	if srcPath == "" {
		srcPath = "slack_backup"
	}

	outDir := mdOutFlag
	if outDir == "" {
		baseDir := srcPath
		if fi, err := os.Stat(srcPath); err == nil && !fi.IsDir() {
			baseDir = filepath.Dir(srcPath)
		}
		outDir = filepath.Join(baseDir, "markdown")
	}

	stats, err := ExecuteExportMD(ctx, srcPath, outDir, cfg.Log)
	if err != nil {
		base.SetExitStatus(base.SApplicationError)
		return err
	}

	cfg.Log.InfoContext(ctx, "Markdown export completed",
		"source", srcPath,
		"markdown_dir", outDir,
		"channels", stats.ChannelsExported,
		"messages", stats.MessagesExported,
		"threads", stats.ThreadsExported,
		"files_linked", stats.FilesLinked,
		"md_files", stats.MarkdownFiles,
	)
	return nil
}

// ExecuteExportMD renders the archive at srcPath into Markdown files inside outDir.
func ExecuteExportMD(ctx context.Context, srcPath, outDir string, lg *slog.Logger) (MDExportStats, error) {
	var stats MDExportStats
	if lg == nil {
		lg = slog.Default()
	}

	src, err := source.Load(ctx, srcPath)
	if err != nil {
		return stats, fmt.Errorf("load archive source %q: %w", srcPath, err)
	}
	defer src.Close()

	archiveRoot := srcPath
	if fi, err := os.Stat(srcPath); err == nil && !fi.IsDir() {
		archiveRoot = filepath.Dir(srcPath)
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return stats, err
	}

	// Build User ID -> Display Name lookup
	userNames := make(map[string]string)
	if users, err := src.Users(ctx); err == nil {
		for _, u := range users {
			userNames[u.ID] = formatUserName(u)
		}
	}

	// Build Channel ID -> Channel Name lookup
	channels, err := src.Channels(ctx)
	if err != nil && !errors.Is(err, source.ErrNotFound) {
		return stats, fmt.Errorf("load channels: %w", err)
	}
	chanNames := make(map[string]string)
	for _, ch := range channels {
		chanNames[ch.ID] = formatChannelDisplayName(ch, userNames)
	}

	// Load optional rescued pins & bookmarks if SQLite DB is available
	var bookmarksByChan map[string][]StoredBookmark
	var pinsByChan map[string][]StoredPin
	if conn, dbErr := bootstrap.Database(srcPath); dbErr == nil {
		bookmarksByChan, _ = LoadBookmarksByChannel(ctx, conn)
		pinsByChan, _ = LoadRescuedPinsByChannel(ctx, conn)
		_ = conn.Close()
	}

	type chanSummary struct {
		ID           string
		DisplayName  string
		DirName      string
		MsgCount     int
		ThreadCount  int
		PinCount     int
		BookmarkCnt  int
		MonthlyFiles []string
	}
	var summaries []chanSummary

	for _, ch := range channels {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		displayName := chanNames[ch.ID]
		safeDirName := source.SanitizeFilename(displayName)
		if safeDirName == "" || safeDirName == "unnamed_file" {
			safeDirName = ch.ID
		}
		chanOutDir := filepath.Join(outDir, "channels", safeDirName)

		var rootMsgs []slack.Message
		repliesByThread := make(map[string][]slack.Message)

		err := IterateChannelMessages(ctx, src, ch.ID, func(msg *slack.Message) error {
			if msg == nil {
				return nil
			}
			m := *msg
			if m.ThreadTimestamp != "" && m.ThreadTimestamp != m.Timestamp {
				repliesByThread[m.ThreadTimestamp] = append(repliesByThread[m.ThreadTimestamp], m)
			} else {
				rootMsgs = append(rootMsgs, m)
			}
			return nil
		})
		if err != nil && !errors.Is(err, source.ErrNotFound) {
			lg.WarnContext(ctx, "warning reading messages for markdown export", "channel", ch.ID, "error", err)
		}

		bms := bookmarksByChan[ch.ID]
		pins := pinsByChan[ch.ID]
		if len(rootMsgs) == 0 && len(bms) == 0 && len(pins) == 0 {
			continue
		}

		if err := os.MkdirAll(chanOutDir, 0o755); err != nil {
			return stats, err
		}

		// Group root messages by YYYY-MM
		msgsByMonth := make(map[string][]slack.Message)
		var months []string
		for _, m := range rootMsgs {
			tm := parseSlackTS(m.Timestamp)
			ym := tm.Format("2006-01")
			if _, exists := msgsByMonth[ym]; !exists {
				months = append(months, ym)
			}
			msgsByMonth[ym] = append(msgsByMonth[ym], m)
		}
		sort.Strings(months)

		var monthlyFiles []string
		chanMsgTotal := 0
		chanThreadTotal := 0

		for _, ym := range months {
			monthMsgs := msgsByMonth[ym]
			mdFileName := ym + ".md"
			mdFilePath := filepath.Join(chanOutDir, mdFileName)

			var sb strings.Builder
			fmt.Fprintf(&sb, "# #%s — %s\n\n", displayName, ym)
			fmt.Fprintf(&sb, "[⬅ 채널 개요(README)로 돌아가기](./README.md) | [🏠 전체 워크스페이스 목차](../../INDEX.md)\n\n---\n\n")

			currentDay := ""
			for _, m := range monthMsgs {
				tm := parseSlackTS(m.Timestamp)
				dayStr := tm.Format("2006-01-02 (Mon)")
				if dayStr != currentDay {
					currentDay = dayStr
					fmt.Fprintf(&sb, "## 📅 %s\n\n", currentDay)
				}

				chanMsgTotal++
				stats.MessagesExported++
				filesLinked := renderMessageMarkdown(&sb, m, "", userNames, chanNames, archiveRoot, chanOutDir)
				stats.FilesLinked += filesLinked

				// Render thread replies if any
				if replies, ok := repliesByThread[m.Timestamp]; ok && len(replies) > 0 {
					chanThreadTotal++
					stats.ThreadsExported++
					fmt.Fprintf(&sb, "\n> 💬 **스레드 댓글 (%d개)**\n>\n", len(replies))
					for _, rep := range replies {
						chanMsgTotal++
						stats.MessagesExported++
						fl := renderMessageMarkdown(&sb, rep, "> ", userNames, chanNames, archiveRoot, chanOutDir)
						stats.FilesLinked += fl
					}
					sb.WriteString("\n")
				}
			}

			if err := os.WriteFile(mdFilePath, []byte(sb.String()), 0o644); err == nil {
				monthlyFiles = append(monthlyFiles, mdFileName)
				stats.MarkdownFiles++
			}
		}

		// Write channel README.md (including Bookmarks & Rescued Pins)
		readmePath := filepath.Join(chanOutDir, "README.md")
		var rsb strings.Builder
		fmt.Fprintf(&rsb, "# #%s (`%s`)\n\n", displayName, ch.ID)
		fmt.Fprintf(&rsb, "[🏠 전체 워크스페이스 목차로 돌아가기](../../INDEX.md)\n\n")
		if ch.Topic.Value != "" {
			fmt.Fprintf(&rsb, "- **토픽(Topic)**: %s\n", FormatSlackMrkdwn(ch.Topic.Value, userNames, chanNames))
		}
		if ch.Purpose.Value != "" {
			fmt.Fprintf(&rsb, "- **목적(Purpose)**: %s\n", FormatSlackMrkdwn(ch.Purpose.Value, userNames, chanNames))
		}
		fmt.Fprintf(&rsb, "- **총 백업 메시지 수**: %d개 (스레드 %d개)\n\n", chanMsgTotal, chanThreadTotal)

		if len(bms) > 0 {
			fmt.Fprintf(&rsb, "## 🔖 채널 북마크 (Bookmarks - %d개)\n\n", len(bms))
			for _, bm := range bms {
				emoji := bm.Emoji
				if emoji == "" {
					emoji = "🔗"
				}
				title := bm.Title
				if title == "" {
					title = bm.Link
				}
				fmt.Fprintf(&rsb, "- %s [%s](%s)\n", emoji, title, bm.Link)
			}
			rsb.WriteString("\n")
		}

		if len(pins) > 0 {
			fmt.Fprintf(&rsb, "## 📌 고정 및 중요 구출 항목 (Pins & Stars - %d개)\n\n", len(pins))
			for _, pin := range pins {
				var msg slack.Message
				if err := json.Unmarshal([]byte(pin.RawJSON), &msg); err == nil && msg.Timestamp != "" {
					_ = renderMessageMarkdown(&rsb, msg, "", userNames, chanNames, archiveRoot, chanOutDir)
				}
			}
			rsb.WriteString("\n")
		}

		fmt.Fprintf(&rsb, "## 🗂 월별 대화 기록\n\n")
		if len(monthlyFiles) == 0 {
			rsb.WriteString("_기록된 일반 메시지가 없습니다._\n")
		} else {
			for _, mf := range monthlyFiles {
				label := strings.TrimSuffix(mf, ".md")
				fmt.Fprintf(&rsb, "- [📄 %s 대화 기록 (%d개 메시지)](./%s)\n", label, len(msgsByMonth[label]), mf)
			}
		}

		if err := os.WriteFile(readmePath, []byte(rsb.String()), 0o644); err == nil {
			stats.MarkdownFiles++
		}

		stats.ChannelsExported++
		summaries = append(summaries, chanSummary{
			ID:           ch.ID,
			DisplayName:  displayName,
			DirName:      safeDirName,
			MsgCount:     chanMsgTotal,
			ThreadCount:  chanThreadTotal,
			PinCount:     len(pins),
			BookmarkCnt:  len(bms),
			MonthlyFiles: monthlyFiles,
		})
	}

	// Write root INDEX.md
	indexPath := filepath.Join(outDir, "INDEX.md")
	var isb strings.Builder
	wsName := "Slack Workspace"
	if wi, err := src.WorkspaceInfo(ctx); err == nil && wi != nil && wi.Team != "" {
		wsName = fmt.Sprintf("%s (%s)", wi.Team, wi.TeamID)
	}
	fmt.Fprintf(&isb, "# 📦 %s — 전체 백업 아카이브\n\n", wsName)
	fmt.Fprintf(&isb, "- **생성 시각**: %s\n", time.Now().Format("2006-01-02 15:04:05 MST"))
	fmt.Fprintf(&isb, "- **백업된 채널/대화방**: %d개\n", stats.ChannelsExported)
	fmt.Fprintf(&isb, "- **총 백업 메시지(댓글 포함)**: %d개\n", stats.MessagesExported)
	fmt.Fprintf(&isb, "- **총 백업 스레드**: %d개\n", stats.ThreadsExported)
	fmt.Fprintf(&isb, "- **연결된 첨부파일/이미지**: %d개\n\n", stats.FilesLinked)

	isb.WriteString("## 📋 채널 및 대화방 목록\n\n")
	isb.WriteString("| 채널 / 대화방 | 채널 ID | 메시지 수 | 스레드 수 | 📌 고정/별표 | 🔖 북마크 | 최근 월별 로그 |\n")
	isb.WriteString("| :--- | :--- | ---: | ---: | ---: | ---: | :--- |\n")
	for _, s := range summaries {
		latestLink := "-"
		if len(s.MonthlyFiles) > 0 {
			last := s.MonthlyFiles[len(s.MonthlyFiles)-1]
			latestLink = fmt.Sprintf("[%s](./channels/%s/%s)", strings.TrimSuffix(last, ".md"), s.DirName, last)
		}
		fmt.Fprintf(&isb, "| [**#%s**](./channels/%s/README.md) | `%s` | %d | %d | %d | %d | %s |\n",
			s.DisplayName, s.DirName, s.ID, s.MsgCount, s.ThreadCount, s.PinCount, s.BookmarkCnt, latestLink)
	}

	if err := os.WriteFile(indexPath, []byte(isb.String()), 0o644); err == nil {
		stats.MarkdownFiles++
	}

	return stats, nil
}

func renderMessageMarkdown(sb *strings.Builder, m slack.Message, prefix string, userNames, chanNames map[string]string, archiveRoot, chanOutDir string) (filesLinked int) {
	tm := parseSlackTS(m.Timestamp)
	timeStr := tm.Format("2006-01-02 15:04:05")
	sender := resolveSender(m, userNames)
	body := FormatSlackMrkdwn(m.Text, userNames, chanNames)

	pinBadge := ""
	if len(m.PinnedTo) > 0 {
		pinBadge = " 📌"
	}

	fmt.Fprintf(sb, "%s**%s** (`%s`)%s\n", prefix, sender, timeStr, pinBadge)
	if strings.TrimSpace(body) != "" {
		for _, line := range strings.Split(body, "\n") {
			fmt.Fprintf(sb, "%s%s\n", prefix, line)
		}
	}

	// Render file attachments
	for _, f := range m.Files {
		name := f.Name
		if name == "" {
			name = f.Title
		}
		if name == "" {
			name = f.ID
		}
		relFromRoot := source.MattermostFilepath(nil, &slack.File{ID: f.ID, Name: name})
		absFile := filepath.Join(archiveRoot, relFromRoot)
		if _, err := os.Stat(absFile); err == nil {
			relLink, err := filepath.Rel(chanOutDir, absFile)
			if err != nil {
				relLink = absFile
			}
			relLink = filepath.ToSlash(relLink)
			filesLinked++
			if strings.HasPrefix(strings.ToLower(f.Mimetype), "image/") {
				fmt.Fprintf(sb, "%s![%s](%s)\n", prefix, name, relLink)
			} else {
				fmt.Fprintf(sb, "%s- 📎 **첨부파일**: [%s](%s) (%d bytes)\n", prefix, name, relLink, f.Size)
			}
		} else if u := FirstDownloadURL(&f); u != "" {
			fmt.Fprintf(sb, "%s- 📎 **첨부파일 (원격 링크)**: [%s](%s)\n", prefix, name, u)
		} else {
			fmt.Fprintf(sb, "%s- 📎 **첨부파일**: `%s` (ID: `%s`, mode: `%s`)\n", prefix, name, f.ID, f.Mode)
		}
	}

	// Render reactions
	if len(m.Reactions) > 0 {
		var rParts []string
		for _, r := range m.Reactions {
			rParts = append(rParts, fmt.Sprintf(":%s: (%d)", r.Name, r.Count))
		}
		fmt.Fprintf(sb, "%s_리액션: %s_\n", prefix, strings.Join(rParts, "  "))
	}
	fmt.Fprintf(sb, "%s\n", strings.TrimRight(prefix, " "))
	return filesLinked
}

var (
	userMentionRe = regexp.MustCompile(`<@([UW][A-Z0-9]+)(?:\|([^>]+))?>`)
	chanMentionRe = regexp.MustCompile(`<#([CG][A-Z0-9]+)(?:\|([^>]+))?>`)
	linkWithTxtRe = regexp.MustCompile(`<(https?://[^|>]+)\|([^>]+)>`)
	bareLinkRe    = regexp.MustCompile(`<(https?://[^>]+)>`)
)

// FormatSlackMrkdwn converts Slack's raw mrkdwn tokens (<@U123>, <#C123|chan>, <url|label>)
// into human-readable Markdown.
func FormatSlackMrkdwn(text string, userNames, chanNames map[string]string) string {
	if text == "" {
		return ""
	}
	out := userMentionRe.ReplaceAllStringFunc(text, func(match string) string {
		sub := userMentionRe.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		uid := sub[1]
		if name, ok := userNames[uid]; ok && name != "" {
			return "@" + name
		}
		if len(sub) >= 3 && sub[2] != "" {
			return "@" + sub[2]
		}
		return "@" + uid
	})

	out = chanMentionRe.ReplaceAllStringFunc(out, func(match string) string {
		sub := chanMentionRe.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		cid := sub[1]
		if len(sub) >= 3 && sub[2] != "" {
			return "#" + sub[2]
		}
		if name, ok := chanNames[cid]; ok && name != "" {
			return "#" + name
		}
		return "#" + cid
	})

	out = linkWithTxtRe.ReplaceAllString(out, "[$2]($1)")
	out = bareLinkRe.ReplaceAllString(out, "$1")
	out = strings.ReplaceAll(out, "<!here>", "@here")
	out = strings.ReplaceAll(out, "<!channel>", "@channel")
	out = strings.ReplaceAll(out, "<!everyone>", "@everyone")
	out = strings.ReplaceAll(out, "&amp;", "&")
	out = strings.ReplaceAll(out, "&lt;", "<")
	out = strings.ReplaceAll(out, "&gt;", ">")
	return out
}

func formatUserName(u slack.User) string {
	for _, candidate := range []string{
		u.Profile.DisplayName,
		u.RealName,
		u.Name,
		u.ID,
	} {
		if strings.TrimSpace(candidate) != "" {
			return strings.TrimSpace(candidate)
		}
	}
	return u.ID
}

func formatChannelDisplayName(ch slack.Channel, userNames map[string]string) string {
	if ch.Name != "" {
		return ch.Name
	}
	if ch.IsIM && ch.User != "" {
		if uname, ok := userNames[ch.User]; ok {
			return "DM_" + uname
		}
		return "DM_" + ch.User
	}
	return ch.ID
}

func resolveSender(m slack.Message, userNames map[string]string) string {
	if m.User != "" {
		if name, ok := userNames[m.User]; ok && name != "" {
			return "@" + name
		}
		return "@" + m.User
	}
	if m.Username != "" {
		return m.Username
	}
	if m.BotID != "" {
		return "Bot(" + m.BotID + ")"
	}
	return "System"
}

func parseSlackTS(ts string) time.Time {
	if iTS, err := fasttime.TS2int(ts); err == nil {
		return fasttime.Int2Time(iTS).Local()
	}
	return time.Now()
}
