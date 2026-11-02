package app

import (
	"crypto/tls"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
)

const maxUploadSize = 100 * 1024 * 1024 // 100MB

// ServerTLSConfig holds TLS configuration for the HTTP server
type ServerTLSConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string
}

// Validate checks if the TLS configuration is valid
func (c ServerTLSConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.CertFile == "" {
		return errors.New("TLS enabled but certificate file not specified")
	}
	if c.KeyFile == "" {
		return errors.New("TLS enabled but key file not specified")
	}
	// Note: We don't check file existence here because the files may not exist
	// at configuration time (e.g., mounted later in container). The server will
	// fail with a clear error when attempting to start if files are missing.
	return nil
}

// createHTTPServer creates an HTTP server with secure defaults
func createHTTPServer(port int, handler http.Handler, tlsConfig ServerTLSConfig) *http.Server {
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           handler,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1MB
	}

	if tlsConfig.Enabled {
		server.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			CurvePreferences: []tls.CurveID{
				tls.X25519,
				tls.CurveP256,
			},
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			},
		}
	}

	return server
}

// createIndexHandler creates the index page handler with proper HTML escaping
func createIndexHandler(babies []baby.Baby) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")

		for _, b := range babies {
			// Escape baby UID to prevent XSS
			escapedUID := html.EscapeString(b.UID)
			fmt.Fprintf(w, "<video src=\"/video/%v.m3u8\" controls autoplay width=\"1280\" height=\"960\"></video>", escapedUID)
		}
	}
}

// createLogHandler creates the log upload handler with proper error handling and size limits
func createLogHandler(dirs DataDirectories) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filename := filepath.Join(dirs.LogDir, fmt.Sprintf("camlogs-%v.tar.gz", time.Now().Format(time.RFC3339)))

		log.Info().Str("file", filename).Msg("Saving log to file")
		defer r.Body.Close()

		out, err := os.Create(filename)
		if err != nil {
			log.Error().Str("file", filename).Err(err).Msg("Unable to create file")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer out.Close()

		// Limit upload size to prevent resource exhaustion
		limitedReader := io.LimitReader(r.Body, maxUploadSize)
		_, err = io.Copy(out, limitedReader)

		if err != nil {
			log.Error().Str("file", filename).Err(err).Msg("Unable to save received log file")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func serve(babies []baby.Baby, dataDir DataDirectories, tlsConfig ServerTLSConfig, health http.Handler) {
	const port = 8080

	// Index handler
	http.HandleFunc("/", createIndexHandler(babies))

	// Local RTMP health. 200 only when every baby has a publisher with recent media.
	// This is a readiness signal. Restarting the process is not required to recover.
	if health != nil {
		http.Handle("/health/local-rtmp", health)
	}

	// Video files
	http.Handle("/video/", http.StripPrefix("/video/", http.FileServer(http.Dir(dataDir.VideoDir))))

	// Dummy log handler - useful for receiving logs from cam
	// Note: Cam is sending tared archive through curl as binary file
	// TODO: proper handling of Expect: 100-continue
	http.HandleFunc("/log", createLogHandler(dataDir))

	server := createHTTPServer(port, nil, tlsConfig)

	if tlsConfig.Enabled {
		log.Info().Int("port", port).Bool("tls", true).Msg("Starting HTTPS server")
		if err := server.ListenAndServeTLS(tlsConfig.CertFile, tlsConfig.KeyFile); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("HTTPS server error")
		}
	} else {
		log.Info().Int("port", port).Bool("tls", false).Msg("Starting HTTP server")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("HTTP server error")
		}
	}
}
