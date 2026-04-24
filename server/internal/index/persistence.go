package index

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"pixeldb/internal/analyzer"
)

func Save(path string, idx *InvertedIndex) error {
	if idx == nil {
		return fmt.Errorf("cannot save nil index")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create index directory: %w", err)
	}

	payload, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal index: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, payload, 0o644); err != nil {
		return fmt.Errorf("write temp index file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp index file: %w", err)
	}
	return nil
}

func Load(path string, a analyzer.Analyzer) (*InvertedIndex, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var idx InvertedIndex
	if err := json.Unmarshal(bytes, &idx); err != nil {
		return nil, fmt.Errorf("unmarshal index: %w", err)
	}

	if idx.DocLengths == nil {
		idx.DocLengths = make(map[int64]int)
	}
	if idx.Terms == nil {
		idx.Terms = make(map[string]*PostingList)
	}
	idx.SetAnalyzer(a)

	// Recompute statistics from disk payload to keep backward compatibility
	// even if old files miss derived fields.
	idx.recomputeStats()
	for _, postingList := range idx.Terms {
		postingList.DocFreq = len(postingList.Postings)
	}

	return &idx, nil
}
