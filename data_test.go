package data_test

import (
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
	datum := data.NewDatum([]float64{0.1, 0.2, 0.3}, 3, 0, 1, 0, "a", "a", 0)
	log.Printf("datum %v\n", datum)
	err = dt.Insert(datum)
	datum2 := data.NewDatum([]float64{0.2, 0.3, 0.4}, 3, 0, 1, 0, "b", "b", 0)
	err = dt.Insert(datum2)
	datum3 := data.NewDatum([]float64{0.2, 0.3, 0.7}, 3, 0, 1, 0, "c", "c", 0)
	err = dt.Insert(datum3)
	for i := 0; i < 5; i++ {
		dt.Process(true)
	}
	log.Printf("stats %v\n", dt.GetStats())

	assert.Nil(t, err)
}
