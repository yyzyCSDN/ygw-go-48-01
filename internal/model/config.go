package model

import (
	"errors"
	"fmt"
	"time"
)

// EngineConfig captures the tunable parameters of the stream engine. Every
// duration is expressed in milliseconds so the demo feed and tests can drive
// the engine with synthetic event times.
type EngineConfig struct {
	WindowSizeMS    int64
	SessionTimeout  int64
	Partitions      int
	BufferSize      int
	CheckpointEvery int
	IdleTimeoutMS   int64
}

// DefaultConfig returns a configuration suitable for the demo runtime.
func DefaultConfig() EngineConfig {
	return EngineConfig{
		WindowSizeMS:    5000,
		SessionTimeout:  10000,
		Partitions:      4,
		BufferSize:      256,
		CheckpointEvery: 64,
		IdleTimeoutMS:   3000,
	}
}

// Validate checks the configuration invariants. It returns a descriptive
// error when any value would make the engine unusable.
func (c EngineConfig) Validate() error {
	if c.WindowSizeMS <= 0 {
		return errors.New("window size must be positive")
	}
	if c.SessionTimeout <= 0 {
		return errors.New("session timeout must be positive")
	}
	if c.Partitions <= 0 {
		return errors.New("partition count must be positive")
	}
	if c.BufferSize <= 0 {
		return errors.New("buffer size must be positive")
	}
	if c.CheckpointEvery <= 0 {
		return errors.New("checkpoint interval must be positive")
	}
	if c.IdleTimeoutMS <= 0 {
		return errors.New("idle timeout must be positive")
	}
	return nil
}

// Describe renders the configuration as a stable human-readable summary used
// by the startup log.
func (c EngineConfig) Describe() string {
	return fmt.Sprintf(
		"window=%dms session=%dms partitions=%d buffer=%d checkpoint_every=%d idle=%dms",
		c.WindowSizeMS,
		c.SessionTimeout,
		c.Partitions,
		c.BufferSize,
		c.CheckpointEvery,
		c.IdleTimeoutMS,
	)
}

// Duration converts a millisecond count to a time.Duration.
func Duration(ms int64) time.Duration {
	return time.Duration(ms) * time.Millisecond
}
