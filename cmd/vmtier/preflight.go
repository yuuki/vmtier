package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/yuuki/vmtier/internal/archive"
	"github.com/yuuki/vmtier/internal/config"
)

func runPreflightCommand(args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return preflightCommand(ctx, args, os.Stdout, time.Now())
}

func preflightCommand(ctx context.Context, args []string, output io.Writer, now time.Time) error {
	fs := flag.NewFlagSet("preflight", flag.ContinueOnError)
	file := fs.String("config", "", "candidate SQLite service configuration (catalog need not exist)")
	start := fs.Int64("start", 0, "inclusive epoch milliseconds of one representative complete window")
	end := fs.Int64("end", 0, "exclusive epoch milliseconds")
	tenant := fs.String("tenant", "", "configured tenant (required when more than one)")
	source := fs.String("source", "", "configured source ID (required when more than one)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *file == "" || *start == 0 || *end == 0 {
		return errors.New("preflight requires -config FILE -start MS -end MS and no positional arguments")
	}
	c, err := config.Read(*file)
	if err != nil {
		return err
	}
	if c.Catalog.Type != "sqlite" {
		return errors.New("preflight sizes the SQLite service rotating-pack pipeline; use a candidate SQLite configuration")
	}
	if err := c.ValidateArchiveRange(*start, *end); err != nil {
		return err
	}
	cfg := c.Archive(*start, *end)
	if *tenant == "" && len(c.Tenants) == 1 {
		*tenant = c.Tenants[0]
	}
	if *source == "" && len(c.Sources) == 1 {
		*source = c.Sources[0].ID
	}
	cfg.Tenants, cfg.Sources = nil, nil
	for _, candidate := range c.Tenants {
		if candidate == *tenant {
			cfg.Tenants = []string{candidate}
		}
	}
	for _, candidate := range c.Sources {
		if candidate.ID == *source {
			cfg.Sources = []archive.Source{{ID: candidate.ID, URL: candidate.URL}}
		}
	}
	if len(cfg.Tenants) != 1 || len(cfg.Sources) != 1 {
		return errors.New("select exactly one configured -tenant and -source for preflight")
	}
	ctx, cancel := context.WithTimeout(ctx, c.ArchiveTimeout())
	defer cancel()
	result, runErr := archive.Preflight(ctx, cfg, now)
	// Partial observations are useful diagnostics, but never a successful estimate.
	if err := json.NewEncoder(output).Encode(result); err != nil {
		return err
	}
	if runErr != nil && (strings.Contains(runErr.Error(), "limit") || strings.Contains(runErr.Error(), "bounds")) {
		return fmt.Errorf("preflight failed: %w; choose a smaller window only before catalog initialization; an existing catalog window is immutable and requires migration to a new catalog", runErr)
	}
	return runErr
}
