package bridge

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/channels"
)

const (
	downloadTimeout     = 30 * time.Second
	downloadMaxBodySize = 50 * 1024 * 1024 // 50 MB
)

type preparedAttachment struct {
	Type     string
	Filename string
	Path     string
	URL      string
}

func buildInboundPrompt(msg channels.InboundMessage, prepared []preparedAttachment) string {
	var parts []string

	if strings.TrimSpace(msg.ReplyToBody) != "" || strings.TrimSpace(msg.ReplyToSender) != "" {
		parts = append(parts, fmt.Sprintf(
			"Reply context:\nFrom: %s\nMessage: %s",
			defaultString(msg.ReplyToSender, "(unknown)"),
			defaultString(msg.ReplyToBody, "(empty)"),
		))
	}

	if len(prepared) > 0 {
		lines := make([]string, 0, len(prepared))
		for _, attachment := range prepared {
			switch {
			case attachment.Path != "":
				lines = append(lines, fmt.Sprintf("- %s: %s", attachment.Type, attachment.Path))
			case attachment.URL != "":
				lines = append(lines, fmt.Sprintf("- %s: %s", attachment.Type, attachment.URL))
			default:
				lines = append(lines, fmt.Sprintf("- %s: %s", attachment.Type, defaultString(attachment.Filename, "(unnamed)")))
			}
		}
		parts = append(parts, "Attachments:\n"+strings.Join(lines, "\n"))
	}

	body := strings.TrimSpace(msg.Text)
	if body == "" && len(prepared) > 0 {
		body = "Please inspect the attached media and respond."
	}
	if body == "" {
		return strings.Join(parts, "\n\n")
	}

	parts = append(parts, body)
	return strings.Join(parts, "\n\n")
}

func materializeAttachments(mediaDir string, attachments []channels.Attachment) ([]preparedAttachment, error) {
	if len(attachments) == 0 {
		return nil, nil
	}

	if mediaDir == "" {
		mediaDir = filepath.Join(os.TempDir(), "cmux-im-bridge-media")
	}
	if err := os.MkdirAll(mediaDir, 0700); err != nil {
		return nil, fmt.Errorf("create media dir: %w", err)
	}

	result := make([]preparedAttachment, 0, len(attachments))
	for idx, attachment := range attachments {
		prepared := preparedAttachment{
			Type:     attachment.Type,
			Filename: attachment.Filename,
			URL:      attachment.URL,
			Path:     attachment.Path,
		}

		switch {
		case len(attachment.Data) > 0:
			path, err := writeAttachmentData(mediaDir, attachment, idx)
			if err != nil {
				return nil, err
			}
			prepared.Path = path
		case attachment.Path != "":
			prepared.Path = attachment.Path
		case attachment.URL != "":
			path, err := downloadAttachment(mediaDir, attachment, idx)
			if err == nil {
				prepared.Path = path
			}
		}

		result = append(result, prepared)
	}
	return result, nil
}

func writeAttachmentData(mediaDir string, attachment channels.Attachment, idx int) (string, error) {
	filename := attachmentFilename(attachment, idx)
	path := filepath.Join(mediaDir, filename)
	if err := os.WriteFile(path, attachment.Data, 0600); err != nil {
		return "", fmt.Errorf("write attachment %q: %w", filename, err)
	}
	return path, nil
}

func downloadAttachment(mediaDir string, attachment channels.Attachment, idx int) (string, error) {
	u, err := url.Parse(attachment.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return "", fmt.Errorf("invalid attachment URL scheme: %q", attachment.URL)
	}

	ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, attachment.URL, nil)
	if err != nil {
		return "", fmt.Errorf("create download request %q: %w", attachment.URL, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("download attachment %q: %w", attachment.URL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download attachment %q: http %d", attachment.URL, resp.StatusCode)
	}

	filename := attachmentFilename(attachment, idx)
	path := filepath.Join(mediaDir, filename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return "", fmt.Errorf("open attachment file %q: %w", path, err)
	}
	defer file.Close()

	limited := io.LimitReader(resp.Body, downloadMaxBodySize)
	if _, err := io.Copy(file, limited); err != nil {
		return "", fmt.Errorf("copy attachment %q: %w", attachment.URL, err)
	}
	return path, nil
}

func attachmentFilename(attachment channels.Attachment, idx int) string {
	if name := strings.TrimSpace(attachment.Filename); name != "" {
		return sanitizeName(filepath.Base(name))
	}
	rawURL := strings.TrimSpace(attachment.URL)
	urlPath := rawURL
	if u, err := url.Parse(rawURL); err == nil && u.Path != "" {
		urlPath = u.Path
	}
	ext := filepath.Ext(urlPath)
	if ext == "" {
		switch attachment.Type {
		case "image":
			ext = ".png"
		case "file":
			ext = ".bin"
		default:
			ext = ".dat"
		}
	}
	return fmt.Sprintf("%d-%d%s", time.Now().UnixNano(), idx, ext)
}
