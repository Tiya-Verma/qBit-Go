package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/gorilla/websocket"

	"github.com/tiyaverma/qbit-go/internal/engine"
	"github.com/tiyaverma/qbit-go/internal/torrent"
)

// Server is the HTTP + WebSocket API server.
type Server struct {
	engine   *engine.Engine
	router   *chi.Mux
	upgrader websocket.Upgrader
}

// New constructs a Server wired to the given Engine.
func New(eng *engine.Engine) *Server {
	s := &Server{
		engine: eng,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/torrents", s.listTorrents)
		r.Post("/torrents", s.addTorrent)
		r.Get("/torrents/{hash}", s.getTorrent)
		r.Delete("/torrents/{hash}", s.removeTorrent)
		r.Post("/torrents/{hash}/pause", s.pauseTorrent)
		r.Post("/torrents/{hash}/resume", s.resumeTorrent)
		r.Get("/stats", s.globalStats)
		r.Get("/ws", s.handleWebSocket)
		r.Get("/settings", s.getSettings)
		r.Patch("/settings", s.updateSettings)
	})

	s.router = r
	return s
}

// Handler returns the http.Handler for use with http.ListenAndServe.
func (s *Server) Handler() http.Handler { return s.router }

// TorrentSummary is the JSON representation of a torrent in list/update responses.
type TorrentSummary struct {
	Hash          string    `json:"hash"`
	Name          string    `json:"name"`
	State         string    `json:"state"`
	Size          int64     `json:"size"`
	Downloaded    int64     `json:"downloaded"`
	Uploaded      int64     `json:"uploaded"`
	Progress      float64   `json:"progress"`
	DownloadSpeed int64     `json:"downloadSpeed"`
	UploadSpeed   int64     `json:"uploadSpeed"`
	AddedAt       time.Time `json:"addedAt"`
}

func toSummary(t *torrent.Torrent) TorrentSummary {
	return TorrentSummary{
		Hash:          t.File.InfoHashString(),
		Name:          t.File.Name,
		State:         t.State.String(),
		Size:          t.File.TotalLength(),
		Downloaded:    t.Stats.Downloaded,
		Uploaded:      t.Stats.Uploaded,
		Progress:      t.Progress(),
		DownloadSpeed: t.Stats.DownloadSpeed,
		UploadSpeed:   t.Stats.UploadSpeed,
		AddedAt:       t.AddedAt,
	}
}

// apiError writes a JSON error response.
func apiError(w http.ResponseWriter, code int, msg, errCode string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg, "code": errCode}) //nolint:errcheck
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func parseHash(r *http.Request) ([20]byte, error) {
	raw := chi.URLParam(r, "hash")
	b, err := hex.DecodeString(strings.TrimSpace(raw))
	if err != nil || len(b) != 20 {
		return [20]byte{}, fmt.Errorf("invalid info hash")
	}
	var h [20]byte
	copy(h[:], b)
	return h, nil
}

func (s *Server) listTorrents(w http.ResponseWriter, r *http.Request) {
	torrents := s.engine.List()
	summaries := make([]TorrentSummary, len(torrents))
	for i, t := range torrents {
		summaries[i] = toSummary(t)
	}
	writeJSON(w, http.StatusOK, summaries)
}

func (s *Server) addTorrent(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		// Try JSON body (magnet link)
		var body struct {
			Magnet   string `json:"magnet"`
			SavePath string `json:"savePath"`
		}
		if err2 := json.NewDecoder(r.Body).Decode(&body); err2 != nil {
			apiError(w, http.StatusBadRequest, "provide a .torrent file or magnet JSON", "BAD_REQUEST")
			return
		}
		if body.Magnet == "" {
			apiError(w, http.StatusBadRequest, "magnet field is required", "BAD_REQUEST")
			return
		}
		t, err := s.engine.AddMagnet(body.Magnet)
		if err != nil {
			if strings.Contains(err.Error(), "already added") {
				apiError(w, http.StatusConflict, err.Error(), "ALREADY_EXISTS")
				return
			}
			apiError(w, http.StatusBadRequest, err.Error(), "BAD_REQUEST")
			return
		}
		writeJSON(w, http.StatusAccepted, toSummary(t))
		return
	}

	file, _, err := r.FormFile("torrent")
	if err != nil {
		apiError(w, http.StatusBadRequest, "missing torrent field", "BAD_REQUEST")
		return
	}
	defer file.Close()

	t, err := s.engine.Add(file)
	if err != nil {
		if strings.Contains(err.Error(), "already added") {
			apiError(w, http.StatusConflict, err.Error(), "ALREADY_EXISTS")
			return
		}
		apiError(w, http.StatusBadRequest, err.Error(), "BAD_REQUEST")
		return
	}
	writeJSON(w, http.StatusCreated, toSummary(t))
}

func (s *Server) getTorrent(w http.ResponseWriter, r *http.Request) {
	hash, err := parseHash(r)
	if err != nil {
		apiError(w, http.StatusBadRequest, err.Error(), "BAD_REQUEST")
		return
	}
	t, err := s.engine.Get(hash)
	if err != nil {
		apiError(w, http.StatusNotFound, "torrent not found", "NOT_FOUND")
		return
	}
	writeJSON(w, http.StatusOK, toSummary(t))
}

func (s *Server) removeTorrent(w http.ResponseWriter, r *http.Request) {
	hash, err := parseHash(r)
	if err != nil {
		apiError(w, http.StatusBadRequest, err.Error(), "BAD_REQUEST")
		return
	}
	deleteFiles := r.URL.Query().Get("deleteFiles") == "true"
	if err := s.engine.Remove(hash, deleteFiles); err != nil {
		apiError(w, http.StatusNotFound, "torrent not found", "NOT_FOUND")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) pauseTorrent(w http.ResponseWriter, r *http.Request) {
	hash, err := parseHash(r)
	if err != nil {
		apiError(w, http.StatusBadRequest, err.Error(), "BAD_REQUEST")
		return
	}
	if err := s.engine.Pause(hash); err != nil {
		apiError(w, http.StatusNotFound, "torrent not found", "NOT_FOUND")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"state": "paused"})
}

func (s *Server) resumeTorrent(w http.ResponseWriter, r *http.Request) {
	hash, err := parseHash(r)
	if err != nil {
		apiError(w, http.StatusBadRequest, err.Error(), "BAD_REQUEST")
		return
	}
	if err := s.engine.Resume(hash); err != nil {
		apiError(w, http.StatusNotFound, "torrent not found", "NOT_FOUND")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"state": "downloading"})
}

func (s *Server) globalStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.engine.Stats())
}

func (s *Server) getSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"note": "settings endpoint - TODO",
	})
}

func (s *Server) updateSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// WSMessage is the envelope for WebSocket push messages.
type WSMessage struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			stats := s.engine.Stats()
			torrents := s.engine.List()
			summaries := make([]TorrentSummary, len(torrents))
			for i, t := range torrents {
				summaries[i] = toSummary(t)
			}
			conn.WriteJSON(WSMessage{Type: "stats", Payload: stats})           //nolint:errcheck
			conn.WriteJSON(WSMessage{Type: "torrent_update", Payload: summaries}) //nolint:errcheck
		case <-r.Context().Done():
			return
		}
	}
}
