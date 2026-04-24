package index

import "math"

const (
	bm25k1 = 1.2
	bm25b  = 0.75
)

func idf(docFreq, totalDocs int) float64 {
	if docFreq <= 0 || totalDocs <= 0 {
		return 0
	}
	return math.Log((float64(totalDocs-docFreq)+0.5)/(float64(docFreq)+0.5) + 1.0)
}

func scoreBM25(queryTerms []string, rowID int64, idx *InvertedIndex) float64 {
	if idx == nil || len(queryTerms) == 0 || idx.TotalDocs == 0 {
		return 0
	}

	docLen, ok := idx.DocLengths[rowID]
	if !ok {
		return 0
	}

	avgFieldLen := idx.AvgFieldLen
	if avgFieldLen <= 0 {
		avgFieldLen = 1.0
	}

	score := 0.0
	for _, term := range queryTerms {
		postingList, exists := idx.Terms[term]
		if !exists || postingList.DocFreq == 0 {
			continue
		}

		frequency := 0
		for _, posting := range postingList.Postings {
			if posting.RowID == rowID {
				frequency = posting.Frequency
				break
			}
		}
		if frequency == 0 {
			continue
		}

		termIDF := idf(postingList.DocFreq, idx.TotalDocs)
		numerator := float64(frequency) * (bm25k1 + 1.0)
		denominator := float64(frequency) + bm25k1*(1.0-bm25b+bm25b*(float64(docLen)/avgFieldLen))
		score += termIDF * (numerator / denominator)
	}

	return score
}
