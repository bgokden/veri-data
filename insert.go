package data

import (
	"time"

	badger "github.com/dgraph-io/badger/v2"
)

// InsertOption is an interface for insertion options
type InsertOption interface{}

// TTLOption is an insertion option for ttl
type TTLOption struct {
	Duration time.Duration
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

// StreamInsert inserts data in stream
func (dt *Data) StreamInsert(datumStream <-chan *Datum, options ...InsertOption) error {
	for e := range datumStream {
		dt.Insert(e, options...)
	}
	return nil
}
