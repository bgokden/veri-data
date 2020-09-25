package data_test

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"testing"

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
	datum := data.NewDatum([]float64{0.1, 0.2, 0.3}, 3, 0, 1, 0, "a", []byte("a"), 0)
	log.Printf("datum %v\n", datum)
	err = dt.Insert(datum)
	datum2 := data.NewDatum([]float64{0.2, 0.3, 0.4}, 3, 0, 1, 0, "b", []byte("b"), 0)
	err = dt.Insert(datum2)
	datum3 := data.NewDatum([]float64{0.2, 0.3, 0.7}, 3, 0, 1, 0, "c", []byte("c"), 0)
	err = dt.Insert(datum3)
	for i := 0; i < 5; i++ {
		dt.Process(true)
	}
	log.Printf("stats %v\n", dt.GetStats())

	assert.Nil(t, err)

	collector := dt.Search(datum)

	for _, e := range collector.List {
		log.Printf("label: %v score: %v\n", e.Datum.Value.Label, e.Score)
	}

	opt := data.ScoreFuncOption{}
	opt.ScoreFunc = data.VectorMultiplication
	opt.HigherIsBetter = true
	collector2 := dt.Search(datum, opt)

	for _, e := range collector2.List {
		log.Printf("label: %v score: %v\n", e.Datum.Value.Label, e.Score)
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
		datum := data.NewDatum(v.Embedding, uint32(len(v.Embedding)), 0, 1, 0, v.Title, []byte(v.Title), 0)
		if oneDatum == nil && index == count {
			oneDatum = datum
		} else {
			dt.Insert(datum)
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
	opt := data.ScoreFuncOption{}
	opt.ScoreFunc = data.VectorMultiplication
	opt.HigherIsBetter = true
	opt2 := data.LimitOption{
		Limit: 10,
	}
	collector := dt.Search(datum, opt, opt2)
	for _, e := range collector.List {
		log.Printf("label: %v score: %v\n", e.Datum.Value.Label, e.Score)
	}
	assert.Equal(t, opt2.Limit, uint32(len(collector.List)))

	assert.Equal(t, []byte("Every outfit Duchess Kate has worn in 2019"), collector.List[1].Datum.Value.Label)
}
