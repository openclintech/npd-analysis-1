package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bodgit/sevenzip"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
)

type FHIRResource struct {
	ID string `json:"id"`
}

type FileConfig struct {
	Bucket       string
	Object       string
	Table        string
	Dataset      string
	ResourceType string
	Compression  string // "zst" or "7z"
	BatchSize    int
}

type IngestMetrics struct {
	File                string
	Dataset             string
	ResourceType        string
	Compression         string
	CompressedSizeBytes int64
	DownloadMs          int64
	ProcessMs           int64
	TotalMs             int64
	RecordCount         int
	ErrorCount          int
	RecordsPerSec       float64
}

// Files are grouped by resource type (table). Within each group, dataset-a and
// dataset-b run concurrently. Groups run sequentially to avoid concurrent writes
// to the same table, which caused errors in earlier runs.
var fileGroups = [][]FileConfig{
	// organizations
	{
		{"national-provider-directory", "dataset-a/organization_A.ndjson.zst", "organizations", "dataset-a", "organization", "zst", 2000},
		{"national-provider-directory", "dataset-b/organization_B.ndjson.7z", "organizations", "dataset-b", "organization", "7z", 2000},
	},
	// organization_affiliations
	{
		{"national-provider-directory", "dataset-a/organization_affiliation_A.ndjson.zst", "organization_affiliations", "dataset-a", "organization_affiliation", "zst", 5000},
		{"national-provider-directory", "dataset-b/organization_affiliation_B.ndjson.7z", "organization_affiliations", "dataset-b", "organization_affiliation", "7z", 5000},
	},
	// practitioners
	{
		{"national-provider-directory", "dataset-a/practitioner_A.ndjson.zst", "practitioners", "dataset-a", "practitioner", "zst", 500},
		{"national-provider-directory", "dataset-b/practitioner_B.ndjson.7z", "practitioners", "dataset-b", "practitioner", "7z", 500},
	},
	// practitioner_roles
	{
		{"national-provider-directory", "dataset-a/practitioner_role_A.ndjson.zst", "practitioner_roles", "dataset-a", "practitioner_role", "zst", 5000},
		{"national-provider-directory", "dataset-b/practitioner_role_B.ndjson.7z", "practitioner_roles", "dataset-b", "practitioner_role", "7z", 5000},
	},
	// locations
	{
		{"national-provider-directory", "dataset-a/location_A.ndjson.zst", "locations", "dataset-a", "location", "zst", 3000},
		{"national-provider-directory", "dataset-b/location_B.ndjson.7z", "locations", "dataset-b", "location", "7z", 3000},
	},
	// endpoints
	{
		{"national-provider-directory", "dataset-a/endpoint_A.ndjson.zst", "endpoints", "dataset-a", "endpoint", "zst", 5000},
		{"national-provider-directory", "dataset-b/endpoint_B.ndjson.7z", "endpoints", "dataset-b", "endpoint", "7z", 5000},
	},
}

func main() {
	ctx := context.Background()

	poolConfig, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	// Recycle connections every 5 minutes to prevent proxy idle timeout drops
	poolConfig.MaxConnLifetime = 5 * time.Minute
	poolConfig.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	var allMetrics []IngestMetrics

	for _, group := range fileGroups {
		fmt.Printf("\n--- Starting resource group: %s ---\n", group[0].Table)

		var (
			mu      sync.Mutex
			wg      sync.WaitGroup
			failed  bool
			failMu  sync.Mutex
		)

		for _, f := range group {
			wg.Add(1)
			go func(f FileConfig) {
				defer wg.Done()

				fmt.Printf("  Starting %s (%s)...\n", f.Object, f.Dataset)
				m, err := ingestFile(ctx, pool, client, f)
				if err != nil {
					log.Fatalf("FATAL error processing %s: %v — aborting", f.Object, err)
				}

				if m.ErrorCount > 0 {
					failMu.Lock()
					failed = true
					failMu.Unlock()
					log.Fatalf("FATAL: %s had %d insert errors — aborting. Fix the issue and re-run.", f.Object, m.ErrorCount)
				}

				saveMetrics(ctx, pool, m)
				printFileMetrics(m)

				mu.Lock()
				allMetrics = append(allMetrics, m)
				mu.Unlock()
			}(f)
		}

		wg.Wait()

		if failed {
			log.Fatal("Aborting due to errors in previous group.")
		}
	}

	printSummary(allMetrics)
}

func ingestFile(ctx context.Context, pool *pgxpool.Pool, client *storage.Client, f FileConfig) (IngestMetrics, error) {
	totalStart := time.Now()

	m := IngestMetrics{
		File:         filepath.Base(f.Object),
		Dataset:      f.Dataset,
		ResourceType: f.ResourceType,
		Compression:  f.Compression,
	}

	// Get compressed file size from GCS
	attrs, err := client.Bucket(f.Bucket).Object(f.Object).Attrs(ctx)
	if err == nil {
		m.CompressedSizeBytes = attrs.Size
	}

	obj := client.Bucket(f.Bucket).Object(f.Object).Retryer(storage.WithPolicy(storage.RetryAlways))
	gcsReader, err := obj.NewReader(ctx)
	if err != nil {
		return m, fmt.Errorf("gcs open: %w", err)
	}
	defer gcsReader.Close()

	var lineReader io.Reader

	switch f.Compression {
	case "zst":
		zr, err := zstd.NewReader(gcsReader)
		if err != nil {
			return m, fmt.Errorf("zstd reader: %w", err)
		}
		defer zr.Close()
		lineReader = zr

	case "7z":
		fmt.Printf("  [%s] Downloading %s to temp file...\n", f.Dataset, formatBytes(m.CompressedSizeBytes))
		downloadStart := time.Now()

		tmp, err := os.CreateTemp("", "ndh-*.7z")
		if err != nil {
			return m, fmt.Errorf("temp file: %w", err)
		}
		defer os.Remove(tmp.Name())
		defer tmp.Close()

		if _, err := io.Copy(tmp, gcsReader); err != nil {
			return m, fmt.Errorf("download 7z: %w", err)
		}
		m.DownloadMs = time.Since(downloadStart).Milliseconds()
		fmt.Printf("  [%s] Download complete in %s\n", f.Dataset, formatDuration(m.DownloadMs))

		r, err := sevenzip.OpenReader(tmp.Name())
		if err != nil {
			return m, fmt.Errorf("open 7z: %w", err)
		}
		defer r.Close()

		if len(r.File) == 0 {
			return m, fmt.Errorf("empty 7z archive")
		}

		rc, err := r.File[0].Open()
		if err != nil {
			return m, fmt.Errorf("open 7z entry: %w", err)
		}
		defer rc.Close()
		lineReader = rc

	default:
		return m, fmt.Errorf("unknown compression: %s", f.Compression)
	}

	processStart := time.Now()

	scanner := bufio.NewScanner(lineReader)
	scanner.Buffer(make([]byte, 1024*1024), 100*1024*1024) // 100MB max line size

	// Table name is hardcoded in FileConfig, not user input — safe to interpolate
	sql := fmt.Sprintf(`
		INSERT INTO %s (fhir_id, raw_json, source_dataset, source_file)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (fhir_id, source_dataset) DO UPDATE SET
			raw_json = EXCLUDED.raw_json,
			source_file = EXCLUDED.source_file,
			ingested_at = now()
	`, f.Table)

	batch := &pgx.Batch{}

	// Acquire a fresh connection per batch flush to avoid long-lived connection resets
	flushBatch := func() error {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("acquire connection: %w", err)
		}
		defer conn.Release()

		br := conn.SendBatch(ctx, batch)
		defer br.Close()
		for i := 0; i < batch.Len(); i++ {
			if _, err := br.Exec(); err != nil {
				return fmt.Errorf("insert failed on record %d in batch: %w", i, err)
			}
		}
		batch = &pgx.Batch{}
		return nil
	}

	for scanner.Scan() {
		line := scanner.Text()

		var res FHIRResource
		if err := json.Unmarshal([]byte(line), &res); err != nil {
			return m, fmt.Errorf("JSON parse error at record %d: %w", m.RecordCount+1, err)
		}

		batch.Queue(sql, res.ID, line, f.Dataset, filepath.Base(f.Object))

		if batch.Len() >= f.BatchSize {
			if err := flushBatch(); err != nil {
				return m, fmt.Errorf("batch flush at record %d: %w", m.RecordCount, err)
			}
		}

		m.RecordCount++
		if m.RecordCount%50000 == 0 {
			elapsed := time.Since(processStart).Seconds()
			fmt.Printf("  [%s/%s] %s records (%.0f rec/s)\n",
				f.Dataset, f.ResourceType,
				formatInt(m.RecordCount),
				float64(m.RecordCount)/elapsed,
			)
		}
	}

	if batch.Len() > 0 {
		if err := flushBatch(); err != nil {
			return m, fmt.Errorf("final batch flush at record %d: %w", m.RecordCount, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return m, fmt.Errorf("scanner: %w", err)
	}

	m.ProcessMs = time.Since(processStart).Milliseconds()
	m.TotalMs = time.Since(totalStart).Milliseconds()
	if m.ProcessMs > 0 {
		m.RecordsPerSec = float64(m.RecordCount) / (float64(m.ProcessMs) / 1000)
	}

	return m, nil
}

func saveMetrics(ctx context.Context, pool *pgxpool.Pool, m IngestMetrics) {
	_, err := pool.Exec(ctx, `
		INSERT INTO ingestion_runs
			(source_file, source_dataset, resource_type, compression,
			 compressed_size_bytes, download_ms, process_ms, total_ms,
			 record_count, error_count, records_per_sec)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`,
		m.File, m.Dataset, m.ResourceType, m.Compression,
		m.CompressedSizeBytes, m.DownloadMs, m.ProcessMs, m.TotalMs,
		m.RecordCount, m.ErrorCount, m.RecordsPerSec,
	)
	if err != nil {
		log.Printf("failed to save metrics: %v", err)
	}
}

func printFileMetrics(m IngestMetrics) {
	fmt.Printf("\n  DONE %-30s %-12s | records: %s | errors: %d | size: %s | process: %s | throughput: %.0f rec/s\n",
		m.File, m.Dataset,
		formatInt(m.RecordCount),
		m.ErrorCount,
		formatBytes(m.CompressedSizeBytes),
		formatDuration(m.ProcessMs),
		m.RecordsPerSec,
	)
	if m.DownloadMs > 0 {
		fmt.Printf("  Download time: %s\n", formatDuration(m.DownloadMs))
	}
}

func printSummary(metrics []IngestMetrics) {
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("INGESTION SUMMARY")
	fmt.Println(strings.Repeat("=", 100))
	fmt.Printf("%-36s %-12s %-10s %10s %8s %12s %12s %14s %12s\n",
		"File", "Dataset", "Format", "Records", "Errors", "Size", "Download", "Process", "Rec/s")
	fmt.Println(strings.Repeat("-", 100))

	datasetTotals := map[string]struct {
		records int
		errors  int
		ms      int64
	}{}

	for _, m := range metrics {
		downloadStr := "-"
		if m.DownloadMs > 0 {
			downloadStr = formatDuration(m.DownloadMs)
		}
		fmt.Printf("%-36s %-12s %-10s %10s %8d %12s %12s %14s %12.0f\n",
			m.File, m.Dataset, m.Compression,
			formatInt(m.RecordCount),
			m.ErrorCount,
			formatBytes(m.CompressedSizeBytes),
			downloadStr,
			formatDuration(m.ProcessMs),
			m.RecordsPerSec,
		)
		t := datasetTotals[m.Dataset]
		t.records += m.RecordCount
		t.errors += m.ErrorCount
		t.ms += m.TotalMs
		datasetTotals[m.Dataset] = t
	}

	fmt.Println(strings.Repeat("-", 100))
	fmt.Println("TOTALS BY DATASET")
	for dataset, t := range datasetTotals {
		fmt.Printf("  %-12s  records: %s  errors: %d  total time: %s\n",
			dataset,
			formatInt(t.records),
			t.errors,
			formatDuration(t.ms),
		)
	}
	fmt.Println(strings.Repeat("=", 100))
	fmt.Println("Metrics saved to ingestion_runs table.")
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func formatDuration(ms int64) string {
	switch {
	case ms >= 60000:
		return fmt.Sprintf("%dm %ds", ms/60000, (ms%60000)/1000)
	case ms >= 1000:
		return fmt.Sprintf("%.1fs", float64(ms)/1000)
	default:
		return fmt.Sprintf("%dms", ms)
	}
}

func formatInt(n int) string {
	s := fmt.Sprintf("%d", n)
	out := []byte{}
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}
