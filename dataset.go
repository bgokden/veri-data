package data

type Dataset struct {
}

func NewDataset() *Dataset {

	return &Dataset{}
}

func (dts *Dataset) GetData(name string) (*Data, error) {

	return nil, nil
}
