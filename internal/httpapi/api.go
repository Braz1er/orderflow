package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"orderflow/internal/domain"
	"orderflow/internal/store"
)

type API struct {
	db        *store.Store
	redis     *redis.Client
	log       *slog.Logger
	tokenHash [sha256.Size]byte
	rateLimit int
}

func New(db *store.Store, cache *redis.Client, log *slog.Logger, token string, limit int) http.Handler {
	api := &API{db: db, redis: cache, log: log, tokenHash: sha256.Sum256([]byte(token)), rateLimit: limit}
	health := http.NewServeMux()
	health.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	health.HandleFunc("GET /readyz", api.ready)

	routes := http.NewServeMux()
	routes.HandleFunc("POST /v1/products", api.createProduct)
	routes.HandleFunc("GET /v1/products", api.listProducts)
	routes.HandleFunc("GET /v1/products/{sku}", api.getProduct)
	routes.HandleFunc("POST /v1/products/{sku}/restock", api.restock)
	routes.HandleFunc("POST /v1/orders", api.placeOrder)
	routes.HandleFunc("GET /v1/orders", api.listOrders)
	routes.HandleFunc("GET /v1/orders/{id}", api.getOrder)
	routes.HandleFunc("POST /v1/orders/{id}/cancel", api.cancelOrder)
	routes.HandleFunc("GET /v1/orders/{id}/events", api.timeline)
	health.Handle("/v1/", api.protect(routes))
	return api.observe(health)
}

// Expiration and counting happen in one atomic Redis operation. Cap the
// counter after the first rejected request to avoid unbounded increments.
var rateScript = redis.NewScript(`
local count = tonumber(redis.call('GET', KEYS[1]) or '0')
local limit = tonumber(ARGV[1])
if count == 0 then
  redis.call('SET', KEYS[1], 1, 'PX', 60000)
elseif count < limit then
  redis.call('INCR', KEYS[1])
else
  return redis.call('PTTL', KEYS[1])
end
return -1
`)

func (a *API) protect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		hash := sha256.Sum256([]byte(token))
		if !ok || !strings.EqualFold(scheme, "Bearer") || subtle.ConstantTimeCompare(hash[:], a.tokenHash[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "unauthorized", "valid bearer token required")
			return
		}
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		wait, err := rateScript.Run(r.Context(), a.redis, []string{"orderflow:ratelimit:v1:" + host}, a.rateLimit).Int64()
		if err != nil {
			a.log.Warn("rate limit unavailable", "request_id", w.Header().Get("X-Request-ID"), "error", err)
			writeError(w, http.StatusServiceUnavailable, "unavailable", "request limiter unavailable")
			return
		}
		if wait >= 0 {
			w.Header().Set("Retry-After", strconv.FormatInt(max(1, (wait+999)/1000), 10))
			writeError(w, http.StatusTooManyRequests, "rate_limited", "request limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}

func (w *statusWriter) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.ResponseWriter.Write(p)
}

func (a *API) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := rand.Text()
		w.Header().Set("X-Request-ID", id)
		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()
		recorder := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(recorder, r.WithContext(ctx))
		if r.URL.Path != "/healthz" && r.URL.Path != "/readyz" {
			a.log.Info("http request", "request_id", id, "method", r.Method, "path", r.URL.Path,
				"status", recorder.status, "duration_ms", time.Since(start).Milliseconds())
		}
	})
}

func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.db.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "database unavailable")
		return
	}
	if err := a.redis.Ping(ctx).Err(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "redis unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(dst)
	if err == nil {
		err = decoder.Decode(new(any))
		if err == io.EOF {
			return true
		}
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "body must be at most 32 KiB")
	} else {
		writeError(w, http.StatusBadRequest, "invalid_json", "expected one valid JSON object with known fields")
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"code": code, "message": message})
}

func (a *API) handleError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalid):
		writeError(w, http.StatusBadRequest, "invalid_input", err.Error())
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, domain.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusServiceUnavailable, "unavailable", "request timed out")
	default:
		a.log.Error("request failed", "request_id", w.Header().Get("X-Request-ID"), "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
	}
}

func pageLimit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 20, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > 100 {
		return 0, fmt.Errorf("%w: limit must be 1–100", domain.ErrInvalid)
	}
	return limit, nil
}

type page[T any, Cursor comparable] struct {
	Items      []T    `json:"items"`
	NextCursor Cursor `json:"next_cursor,omitempty"`
}

func paginate[T any, Cursor comparable](items []T, limit int, cursor func(T) Cursor) page[T, Cursor] {
	if items == nil {
		items = []T{}
	}
	result := page[T, Cursor]{Items: items}
	if len(items) > limit {
		result.Items = items[:limit]
		result.NextCursor = cursor(items[limit-1])
	}
	return result
}
