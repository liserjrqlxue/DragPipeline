package main

import (
	"flag"
	"fmt"
	simpleUtil "github.com/liserjrqlxue/simple-util"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
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
	fq1 = flag.String(
		"fq1",
		"",
		"fq1",
	)
	fq2 = flag.String(
		"fq2",
		"",
		"fq2",
	)
	barcode = flag.String(
		"barcode",
		"",
		"barcode",
	)
	subDir = flag.String(
		"subdir",
		"raw",
		"output split data to outdir/sampleID/subDir/",
	)
	cpuProfile = flag.String(
		"cpu",
		"",
		"cpu profile",
	)
	memProfile = flag.String(
		"mem",
		"",
		"mem profile",
	)
)

var err error

func main() {
	log.Printf("Start:%+v", os.Args)
	flag.Parse()
	if *input == "" || *fq1 == "" || *fq2 == "" {
		flag.Usage()
		log.Printf("-list,-fq1,-fq2 required!")
		os.Exit(0)
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		simpleUtil.CheckErr(pprof.StartCPUProfile(f))
		defer pprof.StopCPUProfile()
	}

	key := strings.Join([]string{*barcode, *fq1, *fq2}, "\t")
	pe := &PE{}
	pe.create(*barcode, key, *fq1, *fq2)

	inputInfo, _ := simpleUtil.File2MapArray(*input, "\t", nil)
	var SampleInfo = make(map[string]*Sample)
	var barcodeMap = make(map[string]string)
	var wg sync.WaitGroup
	for _, item := range inputInfo {
		sampleID := item["sampleID"]
		// SampleInfo
		sample, ok := SampleInfo[sampleID]
		if ok {
			log.Fatalf("sample[%s] duplicate", sampleID)
		} else {
			sample = &Sample{}
			sample.create(item, key, filepath.Join(*outDir, sampleID, *subDir))
			SampleInfo[sampleID] = sample
			wg.Add(1)
			go sample.write(&wg)
		}
		barcodeMap[sample.NewPrimer] = sampleID
	}

	throttle = make(chan bool, 1e6)
	log.Printf("load pe[%s]", pe.Key)
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
		throttle <- true
		n = runtime.NumGoroutine()
		if maxNumGoroutine < n {
			maxNumGoroutine = n
		}
		readName1 := strings.Split(read1[0], "/")[0]
		readName2 := strings.Split(read2[0], "/")[0]
		if readName1 != readName2 {
			log.Fatalf("PE:%d[%s!=%s]", pe.peNo, readName1, readName2)
		}
		sample1, ok1 := barcodeMap[read1[1][:7]]
		sample2, ok2 := barcodeMap[read1[1][:7]]
		if ok1 && ok2 {
			if sample1 != sample2 {
				pe.diffIndex++
				<-throttle
			} else {
				pe.hitNo++
				sample := SampleInfo[sample1]
				go splitReads(read1, read2, sample)
			}
		} else if ok1 || ok2 {
			pe.singleIndex++
			<-throttle
		} else {
			pe.nonIndex++
			<-throttle
		}
	}
	simpleUtil.CheckErr(pe.S1.Err())
	simpleUtil.CheckErr(pe.S2.Err())
	log.Printf("close pe[%s]", pe.Key)
	pe.close()

	log.Printf("split finish:%d", runtime.NumGoroutine())
	for i := 0; i < 1e6; i++ {
		throttle <- true
	}
	log.Printf("split finish:%d", runtime.NumGoroutine())

	// wait close done
	for _, sample := range SampleInfo {
		go sample.close()
	}

	log.Printf("wait for done\n")
	// wait write done
	wg.Wait()

	log.Printf("sampleID\thitNum\twritenum\n")
	for _, sample := range SampleInfo {
		log.Printf("%s\t%d\t%d\n", sample.SampleID, sample.hitNum, sample.writeNum)
	}

	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Fatal(err)
		}
		simpleUtil.CheckErr(pprof.WriteHeapProfile(f))
		defer simpleUtil.DeferClose(f)
	}
	log.Printf("End")
	defer log.Printf("maxGoroutine:%d", maxNumGoroutine)
	fmt.Println(strings.Join([]string{"Barcode", "拆之前reads num", "两端相同index", "两端不同index", "只有一端有index", "两端都没有index", "有效数据利用率"}, "\t"))
	fmt.Printf("%s\t%d\t%d\t%d\t%d\t%d\t%f\n", pe.barcode, pe.peNo, pe.hitNo, pe.diffIndex, pe.singleIndex, pe.nonIndex, float64(pe.hitNo)/float64(pe.peNo))
}
