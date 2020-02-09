package data_test

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	data "github.com/bgokden/veri-data/pkg/data"
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

	assert.Nil(t, err)
}
