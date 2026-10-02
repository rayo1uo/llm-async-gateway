// Package jsonl parses OpenAI Batch input files.
package jsonl

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

const maxLineBytes = 1 << 20

// Parse reads a batch JSONL stream. Structural problems are returned as batch
// errors; a non-nil error means the reader itself failed. expectedEndpoint, when
// non-empty, must match every line's url.
func Parse(r io.Reader, expectedEndpoint string) ([]model.InputLine, []model.BatchError, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), maxLineBytes)

	var (
		lines []model.InputLine
		errs  []model.BatchError
		seen  = map[string]int{}
	)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		raw := bytes.TrimSpace(sc.Bytes())
		if lineNo == 1 {
			raw = bytes.TrimPrefix(raw, []byte{0xEF, 0xBB, 0xBF})
		}
		if len(raw) == 0 {
			continue
		}
		var line model.InputLine
		if err := json.Unmarshal(raw, &line); err != nil {
			errs = append(errs, lineErr(lineNo, "invalid_json", fmt.Sprintf("line %d is not valid JSON: %s", lineNo, err)))
			continue
		}
		line.Index = lineNo
		if msg := validateLine(&line, expectedEndpoint); msg != "" {
			errs = append(errs, lineErr(lineNo, "invalid_request", msg))
			continue
		}
		if prev, ok := seen[line.CustomID]; ok {
			errs = append(errs, lineErr(lineNo, "duplicate_custom_id", fmt.Sprintf("custom_id %q duplicates line %d", line.CustomID, prev)))
			continue
		}
		seen[line.CustomID] = lineNo
		lines = append(lines, line)
	}
	if err := sc.Err(); err != nil {
		return nil, nil, fmt.Errorf("scan jsonl: %w", err)
	}
	if lineNo == 0 || (len(lines) == 0 && len(errs) == 0) {
		errs = append(errs, lineErr(0, "empty_file", "input file has no requests"))
	}
	return lines, errs, nil
}

func validateLine(line *model.InputLine, expectedEndpoint string) string {
	if strings.TrimSpace(line.CustomID) == "" {
		return "custom_id is required"
	}
	if len(line.CustomID) > 512 {
		return "custom_id exceeds 512 characters"
	}
	if line.Method != "POST" {
		return "method must be POST"
	}
	if strings.TrimSpace(line.URL) == "" {
		return "url is required"
	}
	if !model.AllowedEndpoint(line.URL) {
		return fmt.Sprintf("url %q is not a supported endpoint", line.URL)
	}
	if expectedEndpoint != "" && line.URL != expectedEndpoint {
		return fmt.Sprintf("url %q does not match batch endpoint %q", line.URL, expectedEndpoint)
	}
	if len(bytes.TrimSpace(line.Body)) == 0 || !json.Valid(line.Body) {
		return "body must be a JSON value"
	}
	trimmed := bytes.TrimLeftFunc(line.Body, unicode.IsSpace)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return "body must be a JSON object"
	}
	return ""
}

func lineErr(line int, code, message string) model.BatchError {
	e := model.BatchError{Code: code, Message: message, Param: "input"}
	if line > 0 {
		n := line
		e.Line = &n
	}
	return e
}
