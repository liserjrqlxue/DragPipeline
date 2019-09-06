package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/liserjrqlxue/simple-util"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type PE struct {
	barcode  string
	Key      string
	Fq1, Fq2 string
	F1, F2   *os.File
	R1, R2   *gzip.Reader
	S1, S2   *bufio.Scanner
	// current pe
	peNo, hitNo, diffIndex, singleIndex, nonIndex uint64
}

func (pe *PE) create(barcode, key, fq1, fq2 string) {
	pe.barcode = barcode
	pe.Key = key
	pe.Fq1 = fq1
	pe.Fq2 = fq2
	pe.F1, pe.R1, pe.S1 = readFq(fq1)
	pe.F2, pe.R2, pe.S2 = readFq(fq2)
}

func (pe *PE) close() {
	simple_util.CheckErr(pe.R1.Close())
	simple_util.CheckErr(pe.R2.Close())
	simple_util.CheckErr(pe.F1.Close())
	simple_util.CheckErr(pe.F2.Close())
}

type Sample struct {
	SampleID                     string
	barcode                      string
	primer                       string
	NewPrimer                    string
	peKey                        string
	Fq1, Fq2                     string
	F1, F2                       *os.File
	W1, W2                       *gzip.Writer
	hitNum, writeNum             uint64
	hitMutex, writeMutex, WMutex sync.Mutex
	FQ                           chan [2]string
}

func (sample *Sample) create(item map[string]string, peKey, outdir string) {
	sample.SampleID = item["sampleID"]
	sample.primer = item["primer"]
	sample.NewPrimer = sample.primer[:7]
	sample.peKey = peKey
	simple_util.CheckErr(os.MkdirAll(outdir, 0755))
	sample.Fq1 = filepath.Join(outdir, sample.SampleID+".raw_1.fq.gz")
	sample.Fq2 = filepath.Join(outdir, sample.SampleID+".raw_2.fq.gz")
	sample.FQ = make(chan [2]string)
}

func (sample *Sample) write(wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("start %s", sample.SampleID)
	sample.F1, sample.W1 = writeFq(sample.Fq1)
	defer simple_util.DeferClose(sample.F1)
	defer simple_util.DeferClose(sample.W1)
	sample.F2, sample.W2 = writeFq(sample.Fq2)
	defer simple_util.DeferClose(sample.F2)
	defer simple_util.DeferClose(sample.W2)
	for FQ := range sample.FQ {
		sample.WMutex.Lock()
		_, err = fmt.Fprintln(sample.W1, FQ[0])
		simple_util.CheckErr(err, sample.SampleID, "write fq1 error")
		_, err = fmt.Fprintln(sample.W2, FQ[1])
		simple_util.CheckErr(err, sample.SampleID, "write fq2 error")
		sample.WMutex.Unlock()
		sample.writeMutex.Lock()
		sample.writeNum++
		sample.writeMutex.Unlock()
	}
	log.Printf("finis %s", sample.SampleID)
}

func (sample *Sample) close() {
	for sample.writeNum < sample.hitNum {
		log.Printf("wait sample[%s] write finish:%d/%d", sample.SampleID, sample.writeNum, sample.hitNum)
		time.Sleep(1 * time.Second)
	}
	log.Printf("wait sample[%s] write done:%d/%d\tDone", sample.SampleID, sample.writeNum, sample.hitNum)
	close(sample.FQ)
}

var n, maxNumGoroutine int

var throttle chan bool

func readFq(path string) (file *os.File, reader *gzip.Reader, scanner *bufio.Scanner) {
	var err error
	file, err = os.Open(path)
	simple_util.CheckErr(err)
	reader, err = gzip.NewReader(file)
	simple_util.CheckErr(err)
	scanner = bufio.NewScanner(reader)
	return
}

func writeFq(path string) (file *os.File, writer *gzip.Writer) {
	var err error
	file, err = os.Create(path)
	simple_util.CheckErr(err)
	writer = gzip.NewWriter(file)
	simple_util.CheckErr(err)
	return
}

func splitReads(read1, read2 [4]string, sample *Sample) {
	defer func() { <-throttle }()
	read1[1] = read1[1][8:]
	read2[1] = read2[1][8:]
	read1[3] = read1[3][8:]
	read2[3] = read2[3][8:]
	var FQ [2]string
	FQ[0] = strings.Join(read1[:], "\n")
	FQ[1] = strings.Join(read2[:], "\n")
	//go func() { sample.FQ <- FQ }()
	//sample.FQ <- FQ
	sample.hitMutex.Lock()
	sample.hitNum++
	sample.hitMutex.Unlock()
	go func(sample *Sample) { sample.FQ <- FQ }(sample)
}
