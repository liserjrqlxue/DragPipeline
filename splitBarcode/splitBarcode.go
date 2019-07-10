package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/liserjrqlxue/simple-util"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
)

var (
	input = flag.String(
		"input",
		"",
		"input list",
	)
	outDir = flag.String(
		"outdir",
		".",
		"outdir",
	)
	subDir = flag.String(
		"subdir",
		"raw",
		"output split data to outdir/sampleID/subDir/",
	)
	cpuprofile = flag.String(
		"cpuprofile",
		"",
		"cpu profile",
	)
	memprofile = flag.String(
		"memprofile",
		"",
		"mem profile",
	)
)

var err error

type PE struct {
	Key      string
	Fq1, Fq2 string
	F1, F2   *os.File
	R1, R2   *gzip.Reader
	S1, S2   *bufio.Scanner
	// current pe
	peNo   uint64
	peName string
}

func (pe *PE) create(key, fq1, fq2 string) {
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
	SampleID     string
	barcode      string
	pL, pR       string
	NewpL, NewpR string
	peKey        string
	Fq1, Fq2     string
	F1, F2       *os.File
	W1, W2       *gzip.Writer
	hitNum       uint64
}

func (sample *Sample) create(item map[string]string, peKey, outdir string) {
	sample.SampleID = item["sampleID"]
	sample.pL = item["pL"]
	sample.pR = item["pR"]
	sample.NewpL = sample.pL[:7]
	sample.NewpR = sample.pR[:7]
	sample.peKey = peKey
	simple_util.CheckErr(os.MkdirAll(outdir, 0755))
	sample.Fq1 = filepath.Join(outdir, sample.SampleID+".raw_1.fq.gz")
	sample.Fq2 = filepath.Join(outdir, sample.SampleID+".raw_2.fq.gz")
	sample.F1, sample.W1 = writeFq(sample.Fq1)
	sample.F2, sample.W2 = writeFq(sample.Fq2)
}

func (sample *Sample) close() {
	simple_util.CheckErr(sample.W1.Close())
	simple_util.CheckErr(sample.W2.Close())
	simple_util.CheckErr(sample.F1.Close())
	simple_util.CheckErr(sample.F2.Close())
}

func main() {
	log.Printf("Start:%+v", os.Args)
	flag.Parse()
	if *input == "" {
		flag.Usage()
		log.Printf("-list required!")
		os.Exit(0)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	inputInfo, _ := simple_util.File2MapArray(*input, "\t", nil)
	var SampleInfo = make(map[string]*Sample)
	var barcodeMap = make(map[string]string)
	var FqInfo = make(map[string]*PE)
	for _, item := range inputInfo {
		sampleID := item["sampleID"]
		key := strings.Join([]string{item["barcode"], item["fq1"], item["fq2"]}, "\t")

		// SampleInfo
		sample, ok := SampleInfo[sampleID]
		if ok {
			log.Fatalf("sample[%s] duplicate", sampleID)
		} else {
			sample = &Sample{}
			sample.create(item, key, filepath.Join(*outDir, sampleID, *subDir))
			SampleInfo[sampleID] = sample
		}
		barcodeMap[sample.NewpL] = sampleID
		barcodeMap[sample.NewpR] = sampleID

		// FqInfo
		pe, ok := FqInfo[key]
		if !ok {
			pe = &PE{}
			pe.create(key, item["fq1"], item["fq2"])
			FqInfo[key] = pe
		}
	}

	for key, pe := range FqInfo {
		log.Printf("split[%s]", key)
		var loop = true
		var read1, read2 [4]string
		for loop {
			for i := 0; i < 4; i++ {
				loop = pe.S1.Scan() && pe.S2.Scan()
				if !loop {
					break
				}
				read1[i] = pe.S1.Text()
				read2[i] = pe.S2.Text()
			}
			if !loop {
				break
			}
			pe.peNo++
			readName1 := strings.Split(read1[0], "/")[0]
			readName2 := strings.Split(read2[0], "/")[0]
			if readName1 != readName2 {
				log.Fatalf("PE:%d[%s!=%s]", pe.peNo, readName1, readName2)
			} else {
				pe.peName = readName1
			}
			sample1, ok1 := barcodeMap[read1[1][:7]]
			sample2, ok2 := barcodeMap[read1[1][:7]]
			if !ok1 || !ok2 {
				continue
			}
			if sample1 != sample2 {
				log.Fatalf(
					"different Samples[%s:%svs%s:%s] from sample PE[%s:%d]",
					sample1, read1[1][:7],
					sample2, read2[1][:7],
					pe.peName, pe.peNo,
				)
			}
			sample := SampleInfo[sample1]
			sample.hitNum++
			read1[1] = read1[1][8:]
			read2[1] = read2[1][8:]
			read1[3] = read1[3][8:]
			read2[3] = read2[3][8:]
			_, err = fmt.Fprintln(sample.W1, strings.Join(read1[:], "\n"))
			simple_util.CheckErr(err, sample.SampleID, "write fq1 error")
			_, err = fmt.Fprintln(sample.W2, strings.Join(read2[:], "\n"))
			simple_util.CheckErr(err, sample.SampleID, "write fq1 error")
		}
		simple_util.CheckErr(pe.S1.Err())
		simple_util.CheckErr(pe.S2.Err())
	}

	// close()
	for _, pe := range FqInfo {
		log.Printf("close pe[%s]", pe.Key)
		defer pe.close()
	}
	for _, sample := range SampleInfo {
		log.Printf("close sample[%s]", sample.SampleID)
		defer sample.close()
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		defer simple_util.DeferClose(f)
	}
	log.Printf("End")
}

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
