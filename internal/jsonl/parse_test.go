package jsonl

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	okBody := `{"model":"m","messages":[{"role":"user","content":"hi"}]}`
	tests := []struct {
		name     string
		in       string
		endpoint string
		wantN    int
		wantErr  string
	}{
		{
			name: "two lines",
			in: strings.Join([]string{
				`{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":` + okBody + `}`,
				`{"custom_id":"b","method":"POST","url":"/v1/chat/completions","body":` + okBody + `}`,
			}, "\n"),
			endpoint: "/v1/chat/completions",
			wantN:    2,
		},
		{
			name:     "blank lines ignored",
			in:       "\n{\"custom_id\":\"a\",\"method\":\"POST\",\"url\":\"/v1/embeddings\",\"body\":" + okBody + "}\n\n",
			endpoint: "/v1/embeddings",
			wantN:    1,
		},
		{
			name:    "empty",
			in:      "\n\n",
			wantErr: "empty_file",
		},
		{
			name:    "bad json",
			in:      "{nope}\n",
			wantErr: "invalid_json",
		},
		{
			name:    "duplicate",
			in:      `{"custom_id":"a","method":"POST","url":"/v1/completions","body":` + okBody + "}\n" + `{"custom_id":"a","method":"POST","url":"/v1/completions","body":` + okBody + "}\n",
			wantErr: "duplicate_custom_id",
		},
		{
			name:     "endpoint mismatch",
			in:       `{"custom_id":"a","method":"POST","url":"/v1/completions","body":` + okBody + "}\n",
			endpoint: "/v1/chat/completions",
			wantErr:  "invalid_request",
		},
		{
			name:    "body must be object",
			in:      `{"custom_id":"a","method":"POST","url":"/v1/completions","body":[1]}` + "\n",
			wantErr: "invalid_request",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lines, errs, err := Parse(strings.NewReader(tt.in), tt.endpoint)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if tt.wantErr == "" {
				if len(errs) != 0 {
					t.Fatalf("unexpected errors: %+v", errs)
				}
				if len(lines) != tt.wantN {
					t.Fatalf("lines=%d want %d", len(lines), tt.wantN)
				}
				return
			}
			if len(errs) == 0 || errs[0].Code != tt.wantErr {
				t.Fatalf("errors=%+v want code %s", errs, tt.wantErr)
			}
		})
	}
}
