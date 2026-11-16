package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeJSONRequest(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
		body        string
		want        int
	}{
		{"valid JSON", "application/json; charset=utf-8", `{"quantity":2}`, http.StatusOK},
		{"unknown field", "application/json", `{"quantity":2,"other":true}`, http.StatusBadRequest},
		{"second value", "application/json", `{"quantity":2} {}`, http.StatusBadRequest},
		{"wrong media type", "text/plain", `{"quantity":2}`, http.StatusUnsupportedMediaType},
		{"large trailing data", "application/json", `{"quantity":2}` + strings.Repeat(" ", 32<<10), http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.contentType)
			recorder := httptest.NewRecorder()
			var input struct {
				Quantity int `json:"quantity"`
			}
			decoded := decode(recorder, req, &input)
			if decoded != (tc.want == http.StatusOK) || (decoded && input.Quantity != 2) {
				t.Fatalf("decode returned %t with input %+v", decoded, input)
			}
			if recorder.Code != tc.want {
				t.Fatalf("status = %d, want %d, body = %s", recorder.Code, tc.want, recorder.Body.String())
			}
		})
	}
}
