package index

import "pixeldb/internal/analyzer"

type Posting struct {
	RowID     int64 `json:"row_id"`
	Frequency int   `json:"frequency"`
	Positions []int `json:"positions"`
}

type PostingList struct {
	DocFreq  int       `json:"doc_freq"`
	Postings []Posting `json:"postings"`
}

type InvertedIndex struct {
	Column      string                  `json:"column"`
	TotalDocs   int                     `json:"total_docs"`
	AvgFieldLen float64                 `json:"avg_field_len"`
	DocLengths  map[int64]int           `json:"doc_lengths"`
	Terms       map[string]*PostingList `json:"terms"`

	analyzer analyzer.Analyzer `json:"-"`
}

type SearchResult struct {
	RowID int64
	Score float64
}
