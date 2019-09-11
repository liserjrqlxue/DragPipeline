package main

import (
	simple_util "github.com/liserjrqlxue/simple-util"
	"log"
	"path/filepath"
)

func parseInput(input, outDir string) (info Info) {
	info = Info{
		SampleMap:  make(map[string]*Sample),
		BarcodeMap: make(map[string]*Barcode),
	}
	inputInfo, _ := simple_util.File2MapArray(input, "\t", nil)
	for _, item := range inputInfo {
		sampleID := item["sampleID"]
		barcode := item["barcode"]

		fq1 := item["fq1"]
		fq2 := item["fq2"]
		sampleNum := item["sampleNum"]
		primer := item["primer"]

		sampleInfo, ok := info.SampleMap[sampleID]
		if !ok {
			sampleInfo = &Sample{
				sampleID:  sampleID,
				sampleNum: sampleNum,
				barcode:   barcode,
				primer:    primer,
				info:      item,
			}
			info.SampleMap[sampleID] = sampleInfo
		} else {
			log.Fatal("dup sampleID:", sampleID)
		}

		barcodeInfo, ok := info.BarcodeMap[barcode]
		if !ok {
			barcodeInfo = &Barcode{
				barcode: barcode,
				list:    filepath.Join(outDir, "barcode", "barcode."+barcode+".list"),
				fq1:     fq1,
				fq2:     fq2,
				samples: make(map[string]*Sample),
			}
			info.BarcodeMap[barcode] = barcodeInfo
		}
		barcodeInfo.samples[sampleID] = sampleInfo
	}
	return
}
