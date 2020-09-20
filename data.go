package data

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	bpb "github.com/dgraph-io/badger/v2/pb"
)

// Data represents a dataset with similar struture
type Data struct {
	Name        string
	Avg         []float64
	N           int64
	MaxDistance float64
	Hist        []float64
	Timestamp   int64
	DB          *badger.DB
	DBPath      string
	Dirty       bool
}

// Stats to share about data
type Stats struct {
	Avg         []float64
	N           int64
	MaxDistance float64
	Hist        []float64
	Timestamp   int64
}

// NewData creates a data struct
func NewData(name, path string) (*Data, error) {
	dt := &Data{
		Name: name,
	}
	log.Printf("Create Data\n")
	dt.DBPath = fmt.Sprintf("%v/%v", path, name)
	db, err := badger.Open(badger.DefaultOptions(dt.DBPath))
	if err != nil {
		return nil, err
	}
	dt.DB = db
	go dt.Run()
	go func() {
		sigint := make(chan os.Signal, 1)

		// interrupt signal sent from terminal
		signal.Notify(sigint, os.Interrupt)
		// sigterm signal sent from orchastrator
		signal.Notify(sigint, syscall.SIGTERM)

		<-sigint

		dt.Close()
	}()
	return dt, nil
}

// Close currently closes underlying kv store
func (dt *Data) Close() error {
	return dt.DB.Close()
}

// SearchOption is an interface for search options
type SearchOption interface{}

// InsertOption is an interface for insertion options
type InsertOption interface{}

// TTLOption is an insertion option for ttl
type TTLOption struct {
	Duration time.Duration
}

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
	List           []ScoredDatum
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

// Senc collects the results
func (c *Collector) Send(list *bpb.KVList) error {
	itemAdded := false
	for _, item := range list.Kv {
		datumKey, _ := ToDatumKey(item.Key)
		score := c.ScoreFunc(datumKey.Feature, c.DatumKey.Feature)
		if uint32(len(c.List)) < c.N {
			datum, _ := ToDatum(item.Key, item.Value)
			scoredDatum := ScoredDatum{
				Datum: datum,
				Score: score,
			}
			c.List = append(c.List, scoredDatum)
			itemAdded = true
		} else if (c.HigherIsBetter && score > c.List[len(c.List)-1].Score) ||
			(!c.HigherIsBetter && score < c.List[len(c.List)-1].Score) {
			datum, _ := ToDatum(item.Key, item.Value)
			scoredDatum := ScoredDatum{
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
func (dt *Data) Search(datum *Datum, options ...SearchOption) []ScoredDatum {
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
	return c.List
}

// Insert inserts data to internal kv store
func (dt *Data) Insert(datum *Datum, options ...InsertOption) error {
	var ttlDuration *time.Duration
	for _, val := range options {
		switch v := val.(type) {
		case TTLOption:
			ttlDuration = &v.Duration
		}
	}
	keyByte, err := datum.GetKey()
	if err != nil {
		return err
	}
	valueByte, err := datum.GetValue()
	if err != nil {
		return err
	}
	err = dt.DB.Update(func(txn *badger.Txn) error {
		if ttlDuration != nil {
			e := badger.NewEntry(keyByte, valueByte).WithTTL(*ttlDuration)
			return txn.SetEntry(e)
		}
		return txn.Set(keyByte, valueByte)
	})
	if err != nil {
		return err
	}
	return nil
}

// Run runs statistical calculation regularly
func (dt *Data) Run() error {
	nextTime := getCurrentTime()
	for {
		if nextTime <= getCurrentTime() {
			secondsToSleep := int64(10) // increment this based on load
			dt.Process(false)
			nextTime = getCurrentTime() + secondsToSleep
			dt.DB.RunValueLogGC(0.7)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

	}
	// return nil
}

// Process runs through keys and calculates statistics
func (dt *Data) Process(force bool) error {
	if dt.Dirty || getCurrentTime()-dt.Timestamp >= 10000 || force {
		log.Printf("Running Process (forced: %v)\n", force)
		n := int64(0)
		distance := 0.0
		maxDistance := 0.0
		avg := make([]float64, 0)
		hist := make([]float64, 64)
		nFloat := float64(dt.N)
		if nFloat == 0 {
			log.Printf("Data size was 0\n")
			nFloat = 1
		}
		histUnit := 1 / nFloat

		err := dt.DB.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				k := item.Key()
				datumKey, err := ToDatumKey(k)
				if err == nil {
					n++
					avg = CalculateAverage(avg, datumKey.Feature, nFloat)
					distance = VectorDistance(dt.Avg, datumKey.Feature)

					if distance > maxDistance {
						maxDistance = distance
					}
					if dt.MaxDistance != 0 {
						index := int((distance / dt.MaxDistance) * 64)
						if index >= 64 {
							index = 63
						}
						hist[index] += histUnit
					}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		dt.Avg = avg
		dt.Hist = hist
		dt.MaxDistance = maxDistance
		dt.N = n
		dt.Timestamp = getCurrentTime()
	}
	dt.Timestamp = getCurrentTime() // update always
	dt.Dirty = false
	return nil
}

// Get Stats out of data
func (dt *Data) GetStats() *Stats {
	return &Stats{
		Avg:         dt.Avg,
		N:           dt.N,
		MaxDistance: dt.MaxDistance,
		Hist:        dt.Hist,
		Timestamp:   dt.Timestamp,
	}
}
