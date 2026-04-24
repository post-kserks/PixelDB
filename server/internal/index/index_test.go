package index

import (
	"path/filepath"
	"testing"

	"pixeldb/internal/analyzer"
)

func TestInvertedIndexSearchAndRemove(t *testing.T) {
	idx := NewInvertedIndex("bio", analyzer.NewStandardAnalyzer())

	idx.AddDocument(1, "warrior king of gondor")
	idx.AddDocument(2, "elven archer and warrior")
	idx.AddDocument(3, "dwarf miner")

	results := idx.Search("warrior king")
	if len(results) < 2 {
		t.Fatalf("expected at least 2 results, got %d", len(results))
	}
	if results[0].RowID != 1 {
		t.Fatalf("expected rowID=1 to be first, got %d", results[0].RowID)
	}

	idx.RemoveDocument(1)
	results = idx.Search("warrior king")
	for _, result := range results {
		if result.RowID == 1 {
			t.Fatalf("removed document still appears in results")
		}
	}
}

func TestInvertedIndexPersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "_index_bio.json")

	original := NewInvertedIndex("bio", analyzer.NewStandardAnalyzer())
	original.AddDocument(10, "dragon rider")
	original.AddDocument(20, "dragon tamer")
	if err := Save(path, original); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := Load(path, analyzer.NewStandardAnalyzer())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	results := loaded.Search("dragon")
	if len(results) != 2 {
		t.Fatalf("expected 2 search results after load, got %d", len(results))
	}
}
