package data

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	bpb "github.com/dgraph-io/badger/v2/pb"
)

// SearchOption is an interface for search options
type SearchOption interface{}

// ScoreFuncOption is a search option for custom search functions
type ScoreFuncOption struct {
	ScoreFunc      func(arr1 []float64, arr2 []float64) float64
	HigherIsBetter bool
}

// LimitOption is a search option for limit number of results
type LimitOption struct {
	Limit uint32
}

// Collector collects results
type Collector struct {
	List           []*ScoredDatum
	ScoreFunc      func(arr1 []float64, arr2 []float64) float64
	MaxScore       float64
	DatumKey       *DatumKey
	N              uint32
	HigherIsBetter bool
}

// ScoredDatum helps to keep Data ordered
type ScoredDatum struct {
	Datum *Datum
	Score float64
}

// Insert add a new scored datum to collector
func (c *Collector) Insert(scoredDatum *ScoredDatum) error {
	itemAdded := false
	if uint32(len(c.List)) < c.N {
		c.List = append(c.List, scoredDatum)
		itemAdded = true
	} else if (c.HigherIsBetter && scoredDatum.Score > c.List[len(c.List)-1].Score) ||
		(!c.HigherIsBetter && scoredDatum.Score < c.List[len(c.List)-1].Score) {
		c.List[len(c.List)-1] = scoredDatum
		itemAdded = true
	}
	if itemAdded {
		if c.HigherIsBetter {
			sort.Slice(c.List, func(i, j int) bool {
				return c.List[i].Score > c.List[j].Score
			})
		} else {
			sort.Slice(c.List, func(i, j int) bool {
				return c.List[i].Score < c.List[j].Score
			})
		}
	}
	return nil
}

// Senc collects the results
func (c *Collector) Send(list *bpb.KVList) error {
	itemAdded := false
	for _, item := range list.Kv {
		datumKey, _ := ToDatumKey(item.Key)
		score := c.ScoreFunc(datumKey.Feature, c.DatumKey.Feature)
		if uint32(len(c.List)) < c.N {
			datum, _ := ToDatum(item.Key, item.Value)
			scoredDatum := &ScoredDatum{
				Datum: datum,
				Score: score,
			}
			c.List = append(c.List, scoredDatum)
			itemAdded = true
		} else if (c.HigherIsBetter && score > c.List[len(c.List)-1].Score) ||
			(!c.HigherIsBetter && score < c.List[len(c.List)-1].Score) {
			datum, _ := ToDatum(item.Key, item.Value)
			scoredDatum := &ScoredDatum{
				Datum: datum,
				Score: score,
			}
			c.List[len(c.List)-1] = scoredDatum
			itemAdded = true
		}
		if itemAdded {
			if c.HigherIsBetter {
				sort.Slice(c.List, func(i, j int) bool {
					return c.List[i].Score > c.List[j].Score
				})
			} else {
				sort.Slice(c.List, func(i, j int) bool {
					return c.List[i].Score < c.List[j].Score
				})
			}
			itemAdded = false
		}
	}
	return nil
}

// Search does a search based on distances of keys
func (dt *Data) Search(datum *Datum, options ...SearchOption) *Collector {
	c := &Collector{}
	c.ScoreFunc = VectorDistance
	c.DatumKey = datum.Key
	c.HigherIsBetter = false
	c.N = 10
	for _, val := range options {
		switch v := val.(type) {
		case ScoreFuncOption:
			c.ScoreFunc = v.ScoreFunc
			c.HigherIsBetter = v.HigherIsBetter
		case LimitOption:
			c.N = v.Limit
		}
	}
	stream := dt.DB.NewStream()
	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 16                     // Set number of goroutines to use for iteration.
	stream.Prefix = nil                   // Leave nil for iteration over the whole DB.
	stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	stream.ChooseKey = nil

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.
	stream.KeyToList = nil

	// -- End of optional settings.

	// Send is called serially, while Stream.Orchestrate is running.

	stream.Send = c.Send

	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		return nil
	}
	// Done.
	return c
}

// Searchable structs support search interface
type Searchable interface {
	StreamSearch(datum *Datum, scoredDatumStream chan<- *ScoredDatum, queryWaitGroup *sync.WaitGroup, options ...SearchOption) error
}

// StreamSearch does a search based on distances of keys
func (dt *Data) StreamSearch(datum *Datum, scoredDatumStream chan<- *ScoredDatum, queryWaitGroup *sync.WaitGroup, options ...SearchOption) error {
	collector := dt.Search(datum, options...)
	for _, i := range collector.List {
		scoredDatumStream <- i
	}
	queryWaitGroup.Done()
	return nil
}

// SuperSearch searches and merges other resources
func (dt *Data) SuperSearch(datum *Datum, scoredDatumStreamOutput chan<- *ScoredDatum, sources []Searchable, options ...SearchOption) error {
	// Search Start
	scoredDatumStream := make(chan *ScoredDatum, 100)
	var queryWaitGroup sync.WaitGroup
	waitChannel := make(chan struct{})
	go func() {
		defer close(waitChannel)
		queryWaitGroup.Wait()
	}()
	// internal
	queryWaitGroup.Add(1)
	go func() {
		dt.StreamSearch(datum, scoredDatumStream, &queryWaitGroup, options...)
	}()
	// external
	for _, source := range sources {
		queryWaitGroup.Add(1)
		source.StreamSearch(datum, scoredDatumStream, &queryWaitGroup, options...)
	}
	// stream merge
	temp, _ := NewTempData("...")
	defer temp.Close()
	dataAvailable := true
	timeLimit := time.After(time.Duration(1000) * time.Millisecond)
	for dataAvailable {
		select {
		case scoredDatum := <-scoredDatumStream:
			temp.Insert(scoredDatum.Datum)
		case <-waitChannel:
			log.Printf("all data finished")
			close(scoredDatumStream)
			for scoredDatum := range scoredDatumStream {
				temp.Insert(scoredDatum.Datum)
			}
			dataAvailable = false
			break
		case <-timeLimit:
			log.Printf("timeout")
			dataAvailable = false
			break
		}
	}
	// Search End
	collector := temp.Search(datum, options...)
	for _, i := range collector.List {
		scoredDatumStreamOutput <- i
	}
	return nil
}
