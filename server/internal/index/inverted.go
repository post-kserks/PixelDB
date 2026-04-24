package index

import (
	"sort"

	"pixeldb/internal/analyzer"
)

func NewInvertedIndex(column string, a analyzer.Analyzer) *InvertedIndex {
	if a == nil {
		a = analyzer.NewStandardAnalyzer()
	}

	return &InvertedIndex{
		Column:     column,
		DocLengths: make(map[int64]int),
		Terms:      make(map[string]*PostingList),
		analyzer:   a,
	}
}

func (idx *InvertedIndex) SetAnalyzer(a analyzer.Analyzer) {
	if a == nil {
		a = analyzer.NewStandardAnalyzer()
	}
	idx.analyzer = a
}

func (idx *InvertedIndex) AddDocument(rowID int64, text string) {
	if idx == nil {
		return
	}
	idx.ensureAnalyzer()

	if _, exists := idx.DocLengths[rowID]; exists {
		idx.RemoveDocument(rowID)
	}

	tokens := idx.analyzer.Analyze(text)
	frequencies := make(map[string]int, len(tokens))
	positions := make(map[string][]int, len(tokens))
	for _, token := range tokens {
		if token.Term == "" {
			continue
		}
		frequencies[token.Term]++
		positions[token.Term] = append(positions[token.Term], token.Position)
	}

	for term, freq := range frequencies {
		postingList, ok := idx.Terms[term]
		if !ok {
			postingList = &PostingList{}
			idx.Terms[term] = postingList
		}

		postingList.Postings = append(postingList.Postings, Posting{
			RowID:     rowID,
			Frequency: freq,
			Positions: positions[term],
		})
		postingList.DocFreq = len(postingList.Postings)
	}

	idx.DocLengths[rowID] = len(tokens)
	idx.recomputeStats()
}

func (idx *InvertedIndex) RemoveDocument(rowID int64) {
	if idx == nil {
		return
	}
	if _, exists := idx.DocLengths[rowID]; !exists {
		return
	}

	for term, postingList := range idx.Terms {
		filtered := postingList.Postings[:0]
		for _, posting := range postingList.Postings {
			if posting.RowID == rowID {
				continue
			}
			filtered = append(filtered, posting)
		}

		if len(filtered) == 0 {
			delete(idx.Terms, term)
			continue
		}
		postingList.Postings = filtered
		postingList.DocFreq = len(filtered)
	}

	delete(idx.DocLengths, rowID)
	idx.recomputeStats()
}

func (idx *InvertedIndex) Search(query string) []SearchResult {
	if idx == nil {
		return nil
	}
	idx.ensureAnalyzer()

	queryTokens := idx.analyzer.Analyze(query)
	queryTerms := uniqueTerms(queryTokens)
	if len(queryTerms) == 0 || idx.TotalDocs == 0 {
		return nil
	}

	candidateSet := make(map[int64]struct{}, 32)
	for _, term := range queryTerms {
		postingList, ok := idx.Terms[term]
		if !ok {
			continue
		}
		for _, posting := range postingList.Postings {
			candidateSet[posting.RowID] = struct{}{}
		}
	}

	results := make([]SearchResult, 0, len(candidateSet))
	for rowID := range candidateSet {
		score := scoreBM25(queryTerms, rowID, idx)
		if score <= 0 {
			continue
		}
		results = append(results, SearchResult{RowID: rowID, Score: score})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Score == results[j].Score {
			return results[i].RowID < results[j].RowID
		}
		return results[i].Score > results[j].Score
	})

	return results
}

func (idx *InvertedIndex) ensureAnalyzer() {
	if idx.analyzer == nil {
		idx.analyzer = analyzer.NewStandardAnalyzer()
	}
}

func (idx *InvertedIndex) recomputeStats() {
	idx.TotalDocs = len(idx.DocLengths)
	if idx.TotalDocs == 0 {
		idx.AvgFieldLen = 0
		return
	}

	totalLen := 0
	for _, length := range idx.DocLengths {
		totalLen += length
	}
	idx.AvgFieldLen = float64(totalLen) / float64(idx.TotalDocs)
}

func uniqueTerms(tokens []analyzer.AnalyzedToken) []string {
	if len(tokens) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(tokens))
	terms := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token.Term == "" {
			continue
		}
		if _, ok := seen[token.Term]; ok {
			continue
		}
		seen[token.Term] = struct{}{}
		terms = append(terms, token.Term)
	}
	return terms
}
