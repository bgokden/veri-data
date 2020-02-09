package data

import (
	"bytes"
	"encoding/gob"
	"log"
)

type Datum struct {
	Key   *DatumKey
	Value *DatumValue
}

type DatumKey struct {
	Feature    []float64
	Dim1       uint32
	Dim2       uint32
	Size1      uint32
	Size2      uint32
	GroupLabel string
}

type DatumValue struct {
	Label     string
	Timestamp int64
}

func NewDatum(feature []float64,
	dim1 uint32,
	dim2 uint32,
	size1 uint32,
	size2 uint32,
	groupLabel string,
	label string,
	timestamp int64,
) *Datum {
	return &Datum{
		Key: &DatumKey{
			Feature:    feature,
			Dim1:       dim1,
			Dim2:       dim2,
			Size1:      size1,
			Size2:      size1,
			GroupLabel: groupLabel,
		},
		Value: &DatumValue{
			Label:     label,
			Timestamp: timestamp,
		},
	}
}

func (datum *Datum) GetKey() ([]byte, error) {
	var byteBuffer bytes.Buffer
	encoder := gob.NewEncoder(&byteBuffer)
	if err := encoder.Encode(*datum.Key); err != nil {
		log.Printf("Encoding error %v\n", err)
		return nil, err
	}
	return byteBuffer.Bytes(), nil
}

func (datum *Datum) GetValue() ([]byte, error) {
	var byteBuffer bytes.Buffer
	encoder := gob.NewEncoder(&byteBuffer)
	if err := encoder.Encode(*datum.Value); err != nil {
		log.Printf("Encoding error %v\n", err)
		return nil, err
	}
	return byteBuffer.Bytes(), nil
}

func ToDatumKey(byteArray []byte) (*DatumKey, error) {
	var element DatumKey
	r := bytes.NewReader(byteArray)
	decoder := gob.NewDecoder(r)
	if err := decoder.Decode(&element); err != nil {
		log.Printf("Decoding error %v\n", err)
		return nil, err
	}
	return &element, nil
}

func ToDatumValue(byteArray []byte) (*DatumValue, error) {
	var element DatumValue
	r := bytes.NewReader(byteArray)
	decoder := gob.NewDecoder(r)
	if err := decoder.Decode(&element); err != nil {
		log.Printf("Decoding error %v\n", err)
		return nil, err
	}
	return &element, nil
}

func ToDatum(key, value []byte) (*Datum, error) {
	keyP, err := ToDatumKey(key)
	if err != nil {
		return nil, err
	}
	valueP, err := ToDatumValue(value)
	if err != nil {
		return nil, err
	}
	return &Datum{
		Key:   keyP,
		Value: valueP,
	}, nil
}
