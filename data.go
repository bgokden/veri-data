package data

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/patrickmn/go-cache"
)

type DataSource interface {
	StreamSearch(datum *Datum, scoredDatumStream chan<- *ScoredDatum, queryWaitGroup *sync.WaitGroup, config *SearchConfig) error
	StreamInsert(datumStream <-chan *InsertDatumWithConfig) error
	Insert(datum *Datum, config *InsertConfig) error
	GetStats() *Stats
	GetID() string
}

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
	Sources     *cache.Cache
	QueryCache  *cache.Cache
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
	dt.Sources = cache.New(5*time.Minute, 10*time.Minute)
	dt.QueryCache = cache.New(5*time.Minute, 10*time.Minute)
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

// NewPreData creates a data struct
func NewPreData(name, path string) *Data {
	dt := &Data{
		Name: name,
	}
	log.Printf("Create Data\n")
	dt.DBPath = fmt.Sprintf("%v/%v", path, name)
	return dt
}

func (dt *Data) InitData() error {
	log.Printf("Create Data\n")
	db, err := badger.Open(badger.DefaultOptions(dt.DBPath))
	if err != nil {
		return err
	}
	dt.DB = db
	dt.Sources = cache.New(5*time.Minute, 10*time.Minute)
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
	return nil
}

// NewTempData return an inmemory badger instance
func NewTempData(name string) (*Data, error) {
	dt := &Data{
		Name: name,
	}
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		return nil, err
	}
	dt.DB = db
	return dt, nil
}

// Close currently closes underlying kv store
func (dt *Data) Close() error {
	return dt.DB.Close()
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

// AddSource adds a source
func (dt *Data) AddSource(dataSource DataSource) {
	dt.Sources.Set(dataSource.GetID(), dataSource, cache.DefaultExpiration)
}

func (dt *Data) GetID() string {
	return dt.Name
}
