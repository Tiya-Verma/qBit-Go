package engine

// EncryptionMode controls protocol encryption preference.
type EncryptionMode int

const (
	EncryptionDisabled  EncryptionMode = iota
	EncryptionPreferred                // use encryption if peer supports it
	EncryptionRequired                 // disconnect peers that don't support encryption
)

// Config holds all tunable engine parameters.
type Config struct {
	ListenPort     int            // TCP port for peer connections (default: 6881)
	APIPort        int            // HTTP API port (default: 8080)
	DownloadDir    string         // default download directory
	MaxConnections int            // global peer connection cap (default: 200)
	MaxPerTorrent  int            // per-torrent peer cap (default: 50)
	GlobalDownSpeed int64         // bytes/sec, 0 = unlimited
	GlobalUpSpeed   int64         // bytes/sec, 0 = unlimited
	DHTEnabled     bool
	PEXEnabled     bool
	EncryptionMode EncryptionMode
	DBPath         string         // path to bbolt database file
}

// DefaultConfig returns a Config with sane defaults.
func DefaultConfig() *Config {
	return &Config{
		ListenPort:     6881,
		APIPort:        8080,
		DownloadDir:    ".",
		MaxConnections: 200,
		MaxPerTorrent:  50,
		DHTEnabled:     true,
		PEXEnabled:     true,
		EncryptionMode: EncryptionPreferred,
		DBPath:         "qbit.db",
	}
}
