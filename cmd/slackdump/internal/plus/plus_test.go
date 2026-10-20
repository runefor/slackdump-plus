package plus

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rusq/slack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rusq/slackdump/v4/cmd/slackdump/internal/bootstrap"
	_ "modernc.org/sqlite"
)

type fakeRescueClient struct {
	filePayload []byte
}

func (f *fakeRescueClient) AuthTestContext(ctx context.Context) (*slack.AuthTestResponse, error) {
	return &slack.AuthTestResponse{
		Team:   "TestTeam",
		TeamID: "T12345",
		User:   "tester",
		UserID: "U11111",
	}, nil
}

func (f *fakeRescueClient) GetConversationHistoryContext(ctx context.Context, params *slack.GetConversationHistoryParameters) (*slack.GetConversationHistoryResponse, error) {
	return &slack.GetConversationHistoryResponse{}, nil
}

func (f *fakeRescueClient) GetConversationInfoContext(ctx context.Context, input *slack.GetConversationInfoInput) (*slack.Channel, error) {
	return &slack.Channel{
		GroupConversation: slack.GroupConversation{
			Conversation: slack.Conversation{ID: input.ChannelID},
			Name:         "general",
		},
	}, nil
}

func (f *fakeRescueClient) GetConversationRepliesContext(ctx context.Context, params *slack.GetConversationRepliesParameters) ([]slack.Message, bool, string, error) {
	parent := slack.Message{
		Msg: slack.Msg{
			User:            "U11111",
			Timestamp:       params.Timestamp,
			ThreadTimestamp: params.Timestamp,
			Text:            "Pinned parent message older than 90 days <@U22222>",
			ReplyCount:      1,
		},
	}
	reply := slack.Message{
		Msg: slack.Msg{
			User:            "U22222",
			Timestamp:       "1700000100.000200",
			ThreadTimestamp: params.Timestamp,
			Text:            "Important reply inside old thread <#C11111|general>",
		},
	}
	return []slack.Message{parent, reply}, false, "", nil
}

func (f *fakeRescueClient) GetConversationsContext(ctx context.Context, params *slack.GetConversationsParameters) ([]slack.Channel, string, error) {
	ch := slack.Channel{
		GroupConversation: slack.GroupConversation{
			Conversation: slack.Conversation{ID: "C11111"},
			Name:         "general",
			Topic:        slack.Topic{Value: "Team announcements"},
			Purpose:      slack.Purpose{Value: "Important notices"},
		},
	}
	return []slack.Channel{ch}, "", nil
}

func (f *fakeRescueClient) GetEmojiContext(ctx context.Context) (map[string]string, error) {
	return map[string]string{}, nil
}

func (f *fakeRescueClient) GetFileContext(ctx context.Context, downloadURL string, writer io.Writer) error {
	_, err := writer.Write(f.filePayload)
	return err
}

func (f *fakeRescueClient) GetFileInfoContext(ctx context.Context, fileID string, count int, page int) (*slack.File, []slack.Comment, *slack.Paging, error) {
	return &slack.File{
		ID:                 fileID,
		Name:               "rescued_architecture.png",
		Mimetype:           "image/png",
		Size:               len(f.filePayload),
		URLPrivateDownload: "https://files.slack.com/files-pri/T123-F999/rescued_architecture.png",
	}, nil, nil, nil
}

func (f *fakeRescueClient) GetUserInfoContext(ctx context.Context, user string) (*slack.User, error) {
	return &slack.User{ID: user, Name: "user_" + user}, nil
}

func (f *fakeRescueClient) GetUsersContext(ctx context.Context, options ...slack.GetUsersOption) ([]slack.User, error) {
	return []slack.User{
		{ID: "U11111", Name: "alice", RealName: "Alice Kim", Profile: slack.UserProfile{DisplayName: "Alice"}},
		{ID: "U22222", Name: "bob", RealName: "Bob Lee", Profile: slack.UserProfile{DisplayName: "Bob"}},
	}, nil
}

func (f *fakeRescueClient) GetUsersInConversationContext(ctx context.Context, params *slack.GetUsersInConversationParameters) ([]string, string, error) {
	return []string{"U11111", "U22222"}, "", nil
}

func (f *fakeRescueClient) GetUsersPaginated(options ...slack.GetUsersOption) slack.UserPagination {
	return slack.UserPagination{}
}

func (f *fakeRescueClient) ListBookmarks(channelID string) ([]slack.Bookmark, error) {
	return []slack.Bookmark{
		{
			ID:        "Bk1001",
			ChannelID: channelID,
			Title:     "Production Runbook",
			Link:      "https://wiki.example.com/runbook",
			Emoji:     ":📘:",
			Type:      "link",
		},
	}, nil
}

func (f *fakeRescueClient) SearchFilesContext(ctx context.Context, query string, params slack.SearchParameters) (*slack.SearchFiles, error) {
	return &slack.SearchFiles{}, nil
}

func (f *fakeRescueClient) SearchMessagesContext(ctx context.Context, query string, params slack.SearchParameters) (*slack.SearchMessages, error) {
	return &slack.SearchMessages{
		Matches: []slack.SearchMessage{
			{
				User:      "U11111",
				Username:  "alice",
				Timestamp: "1700000500.000500",
				Text:      "Search-rescued message for query " + query,
				Channel:   slack.CtxChannel{ID: "C11111", Name: "general"},
			},
		},
		Paging: slack.Paging{Pages: 1},
	}, nil
}

func (f *fakeRescueClient) GetUserProfileContext(ctx context.Context, params *slack.GetUserProfileParameters) (*slack.UserProfile, error) {
	return &slack.UserProfile{}, nil
}

func (f *fakeRescueClient) ListPinsContext(ctx context.Context, channel string) ([]slack.Item, *slack.Paging, error) {
	msg := &slack.Message{
		Msg: slack.Msg{
			User:            "U11111",
			Timestamp:       "1700000000.000100",
			ThreadTimestamp: "1700000000.000100",
			ReplyCount:      1,
			Text:            "Pinned parent message older than 90 days <@U22222>",
			PinnedTo:        []string{channel},
			Files: []slack.File{
				{
					ID:                 "F999",
					Name:               "rescued_architecture.png",
					Mimetype:           "image/png",
					Size:               len(f.filePayload),
					Mode:               "hidden_by_limit", // simulate >90 day hidden flag where URL is still present or fetched
					URLPrivateDownload: "https://files.slack.com/files-pri/T123-F999/rescued_architecture.png",
				},
			},
		},
	}
	return []slack.Item{
		{
			Type:      "message",
			Channel:   channel,
			Message:   msg,
			Timestamp: msg.Timestamp,
		},
	}, &slack.Paging{Pages: 1}, nil
}

func (f *fakeRescueClient) ListStarsContext(ctx context.Context, params slack.StarsParameters) ([]slack.Item, string, error) {
	msg := &slack.Message{
		Msg: slack.Msg{
			User:      "U22222",
			Timestamp: "1700000300.000300",
			Text:      "Starred secret credential note <https://internal.example.com|Internal Portal>",
		},
	}
	return []slack.Item{
		{
			Type:      "message",
			Channel:   "C11111",
			Message:   msg,
			Timestamp: msg.Timestamp,
		},
	}, "", nil
}

func (f *fakeRescueClient) GetFilesContext(ctx context.Context, params slack.GetFilesParameters) ([]slack.File, *slack.Paging, error) {
	return []slack.File{
		{
			ID:                 "F888",
			Name:               "standalone_spec.pdf",
			Mimetype:           "application/pdf",
			Size:               len(f.filePayload),
			Created:            1700000400,
			Channels:           []string{"C11111"},
			URLPrivateDownload: "https://files.slack.com/files-pri/T123-F888/standalone_spec.pdf",
		},
	}, &slack.Paging{Pages: 1}, nil
}

func TestFormatSlackMrkdwn(t *testing.T) {
	users := map[string]string{
		"U11111": "Alice",
		"U22222": "Bob",
	}
	chans := map[string]string{
		"C11111": "general",
	}

	raw := "Hello <@U11111> and <@U22222|bobby>! Check <#C11111|general> or <#C11111> and <https://example.com|Docs> &amp; <!here>"
	got := FormatSlackMrkdwn(raw, users, chans)
	assert.Equal(t, "Hello @Alice and @Bob! Check #general or #general and [Docs](https://example.com) & @here", got)
}

func TestRescueAndExportMDPipeline(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	archiveDir := filepath.Join(tmpDir, "backup")

	fakeCl := &fakeRescueClient{
		filePayload: []byte("\x89PNG\r\n\x1a\nfake-image-payload-bytes"),
	}

	// 1. Run ExecuteRescue (simulating >90-day pins, threads, bookmarks, stars, standalone files, and search)
	rStats, err := ExecuteRescue(ctx, fakeCl, archiveDir, RescueOptions{
		Pins:      true,
		Bookmarks: true,
		Stars:     true,
		FilesScan: true,
		Search:    true,
		MaxPages:  2,
		Dedupe:    true,
	}, true, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, rStats.ChannelsScanned)
	assert.Equal(t, 1, rStats.PinnedMessages)
	assert.Equal(t, 1, rStats.PinnedThreads)
	assert.Equal(t, 1, rStats.BookmarksRescued)
	assert.Equal(t, 1, rStats.StarredItems)
	assert.Equal(t, 1, rStats.StandaloneFiles)
	assert.GreaterOrEqual(t, rStats.FilesDownloaded, 2)

	// Verify that hidden_by_limit file F999 and standalone file F888 were downloaded into __uploads
	assert.FileExists(t, filepath.Join(archiveDir, "__uploads", "F999", "rescued_architecture.png"))
	assert.FileExists(t, filepath.Join(archiveDir, "__uploads", "F888", "standalone_spec.pdf"))

	// 2. Also test ExecuteRescueZip by creating a mock Official Export folder with an even older (>90 days) message & file
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("zip-rescued-document-content"))
	}))
	defer srv.Close()

	exportDir := filepath.Join(tmpDir, "official_export")
	require.NoError(t, os.MkdirAll(filepath.Join(exportDir, "general"), 0o755))

	usersJSON, _ := json.Marshal([]slack.User{
		{ID: "U11111", Name: "alice", RealName: "Alice Kim", Profile: slack.UserProfile{DisplayName: "Alice"}},
		{ID: "U22222", Name: "bob", RealName: "Bob Lee", Profile: slack.UserProfile{DisplayName: "Bob"}},
	})
	require.NoError(t, os.WriteFile(filepath.Join(exportDir, "users.json"), usersJSON, 0o644))

	chansJSON, _ := json.Marshal([]slack.Channel{
		{
			GroupConversation: slack.GroupConversation{
				Conversation: slack.Conversation{ID: "C11111"},
				Name:         "general",
			},
		},
	})
	require.NoError(t, os.WriteFile(filepath.Join(exportDir, "channels.json"), chansJSON, 0o644))

	oldMsgsJSON, _ := json.Marshal([]slack.Message{
		{
			Msg: slack.Msg{
				User:      "U11111",
				Timestamp: "1690000000.000100",
				Text:      "Historical 200-day-old message from official export with <@U22222>",
				Files: []slack.File{
					{
						ID:                 "F777",
						Name:               "old_contract.txt",
						Mimetype:           "text/plain",
						Size:               28,
						URLPrivateDownload: srv.URL + "/old_contract.txt?t=xoxe-signed",
					},
				},
			},
		},
	})
	require.NoError(t, os.WriteFile(filepath.Join(exportDir, "general", "2023-07-22.json"), oldMsgsJSON, 0o644))

	zStats, err := ExecuteRescueZip(ctx, fakeCl, exportDir, archiveDir, true, true, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, zStats.ChannelsMerged)
	assert.Equal(t, 1, zStats.MessagesMerged)
	assert.Equal(t, 1, zStats.FilesDownloaded)
	assert.FileExists(t, filepath.Join(archiveDir, "__uploads", "F777", "old_contract.txt"))

	// 3. Run ExecuteExportMD and verify generated Markdown files
	mdDir := filepath.Join(archiveDir, "markdown")
	mStats, err := ExecuteExportMD(ctx, archiveDir, mdDir, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, mStats.ChannelsExported)
	assert.GreaterOrEqual(t, mStats.MessagesExported, 4)

	indexBytes, err := os.ReadFile(filepath.Join(mdDir, "INDEX.md"))
	require.NoError(t, err)
	assert.Contains(t, string(indexBytes), "#general")

	readmeBytes, err := os.ReadFile(filepath.Join(mdDir, "channels", "general", "README.md"))
	require.NoError(t, err)
	readmeStr := string(readmeBytes)
	assert.Contains(t, readmeStr, "Production Runbook")
	assert.Contains(t, readmeStr, "Pinned parent message older than 90 days @Bob")

	// Verify monthly markdown contains thread reply and relative attachment link
	entries, err := os.ReadDir(filepath.Join(mdDir, "channels", "general"))
	require.NoError(t, err)
	var allMonthly strings.Builder
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".md") && e.Name() != "README.md" {
			b, _ := os.ReadFile(filepath.Join(mdDir, "channels", "general", e.Name()))
			allMonthly.Write(b)
		}
	}
	monthlyContent := allMonthly.String()
	assert.Contains(t, monthlyContent, "Important reply inside old thread #general")
	assert.Contains(t, monthlyContent, "Historical 200-day-old message from official export with @Bob")
	assert.Contains(t, monthlyContent, "__uploads/F999/rescued_architecture.png")

	// Ensure database can be opened cleanly
	conn, err := bootstrap.Database(archiveDir)
	require.NoError(t, err)
	_ = conn.Close()
}
