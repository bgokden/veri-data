package data_test

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	data "github.com/bgokden/veri-data"
	"github.com/stretchr/testify/assert"
)

func TestData(t *testing.T) {
	dir, err := ioutil.TempDir("", "veri-test")
	if err != nil {
		log.Fatal(err)
	}

	defer os.RemoveAll(dir) // clean up

	dt, err := data.NewData("data1", dir)
	assert.Nil(t, err)
	defer dt.Close()
	datum := data.NewDatum([]float64{0.1, 0.2, 0.3}, 3, 0, 1, 0, []byte("a"), []byte("a"), 0)
	log.Printf("datum %v\n", datum)
	err = dt.Insert(datum, nil)
	datum2 := data.NewDatum([]float64{0.2, 0.3, 0.4}, 3, 0, 1, 0, []byte("b"), []byte("b"), 0)
	err = dt.Insert(datum2, nil)
	datum3 := data.NewDatum([]float64{0.2, 0.3, 0.7}, 3, 0, 1, 0, []byte("c"), []byte("c"), 0)
	err = dt.Insert(datum3, nil)
	for i := 0; i < 5; i++ {
		dt.Process(true)
	}
	log.Printf("stats %v\n", dt.GetStats())

	assert.Nil(t, err)

	collector := dt.Search(datum, nil)

	for _, e := range collector.List {
		log.Printf("label: %v score: %v\n", string(e.Datum.Value.Label), e.Score)
	}

	config := data.DefaultSearchConfig()
	config.ScoreFunc = data.VectorMultiplication
	config.HigherIsBetter = true
	collector2 := dt.Search(datum, config)

	for _, e := range collector2.List {
		log.Printf("label: %v score: %v\n", string(e.Datum.Value.Label), e.Score)
	}

}

type NewsTitle struct {
	Title     string
	Embedding []float64
}

func load_data_from_json(dt *data.Data, fname string) (*data.Datum, error) {
	var oneDatum *data.Datum
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	index := 0
	count := 0
	s := bufio.NewScanner(f)
	for s.Scan() {
		var v NewsTitle
		if err := json.Unmarshal(s.Bytes(), &v); err != nil {
			return nil, err
		}
		datum := data.NewDatum(v.Embedding, uint32(len(v.Embedding)), 0, 1, 0, []byte(v.Title), []byte(v.Title), 0)
		if oneDatum == nil && index == count {
			oneDatum = datum
		} else {
			dt.Insert(datum, nil)
		}
		index++
	}
	if s.Err() != nil {
		return nil, s.Err()
	}
	return oneDatum, nil
}

func TestData2(t *testing.T) {
	dir, err := ioutil.TempDir("", "veri-test")
	if err != nil {
		log.Fatal(err)
	}

	defer os.RemoveAll(dir) // clean up

	dt, err := data.NewData("data2", dir)
	assert.Nil(t, err)
	defer dt.Close()

	datum, err := load_data_from_json(dt, "./testdata/news_title_embdeddings.json")
	assert.Nil(t, err)

	for i := 0; i < 5; i++ {
		dt.Process(true)
	}
	log.Printf("stats %v\n", dt.GetStats().N)

	log.Printf("label: %v\n", datum.Value.Label)
	config := data.DefaultSearchConfig()
	config.ScoreFunc = data.VectorMultiplication
	config.HigherIsBetter = true
	config.Limit = 10
	collector := dt.Search(datum, config)
	for _, e := range collector.List {
		log.Printf("label: %v score: %v\n", e.Datum.Value.Label, e.Score)
	}
	assert.Equal(t, config.Limit, uint32(len(collector.List)))

	assert.Equal(t, []byte("Every outfit Duchess Kate has worn in 2019"), collector.List[1].Datum.Value.Label)
}

func TestDataStreamSearch(t *testing.T) {
	dir, err := ioutil.TempDir("", "veri-test")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	dt01, err := data.NewData("data01", dir)
	assert.Nil(t, err)
	defer dt01.Close()

	datum, err := load_data_from_json(dt01, "./testdata/news_title_embdeddings.json")
	assert.Nil(t, err)

	dt02, err := data.NewData("data02", dir)
	assert.Nil(t, err)
	defer dt02.Close()

	_, err = load_data_from_json(dt02, "./testdata/news_title_embdeddings.json")
	assert.Nil(t, err)

	config := data.DefaultSearchConfig()
	config.ScoreFunc = data.VectorMultiplication
	config.HigherIsBetter = true
	config.Limit = 10
	scoredDatumStream := make(chan *data.ScoredDatum, 100)
	dt01.AddSource(dt02)
	err = dt01.SuperSearch(datum, scoredDatumStream, config)
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	close(scoredDatumStream)
	for e := range scoredDatumStream {
		log.Printf("label: %v score: %v\n", string(e.Datum.Value.Label), e.Score)
	}
	rand.Seed(42)
	datumStream := make(chan *data.Datum, 100)
	err = dt01.StreamSample(datumStream, 0.5)
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	close(datumStream)
	log.Printf("Stream Sample\n")
	count := 0
	for e := range datumStream {
		log.Printf("label %v: %v\n", count, string(e.Value.Label))
		count++
	}
	assert.Equal(t, 24, count)

	datumStreamAll := make(chan *data.Datum, 100)
	err = dt01.StreamAll(datumStreamAll)
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	close(datumStreamAll)
	log.Printf("Stream All\n")
	countAll := 0
	for e := range datumStreamAll {
		log.Printf("label %v: %v\n", countAll, string(e.Value.Label))
		countAll++
	}
	assert.Equal(t, 49, countAll)
}
