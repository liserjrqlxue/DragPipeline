package main

type Info struct {
	SampleMap  map[string]*Sample
	BarcodeMap map[string]*Barcode
}

type Barcode struct {
	barcode string
	list    string
	fq1     string
	fq2     string
	samples map[string]*Sample
}

type Sample struct {
	sampleID  string
	sampleNum string
	barcode   string
	primer    string
	info      map[string]string
}
