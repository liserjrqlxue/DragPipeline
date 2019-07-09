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
	"strings"
)

var (
	name = flag.String(
		"name",
		"",
		"sample name",
	)
	barcode = flag.String(
		"barcode",
		"",
		"sample barcode",
	)
	p1 = flag.String(
		"p1",
		"",
		"p1",
	)
	p2 = flag.String(
		"p2",
		"",
		"p2",
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
	outdir = flag.String(
		"outdir",
		"",
		"outdir",
	)
)

var err error

func main() {
	flag.Parse()
	// 7bp
	log.Println(*p1)
	P1 := *p1
	P1 = P1[:7]
	log.Println(P1)
	P2 := *p2
	P2 = P2[:7]
	var barcodeHash = make(map[string]bool)
	barcodeHash[P1] = true
	barcodeHash[P2] = true
	log.Printf("%+v", barcodeHash)

	fq1F, fq1R, fq1S := readFq(*fq1)
	fq2F, fq2R, fq2S := readFq(*fq2)
	defer simple_util.DeferClose(fq1F)
	defer simple_util.DeferClose(fq1R)
	defer simple_util.DeferClose(fq2F)
	defer simple_util.DeferClose(fq2R)

	simple_util.CheckErr(os.MkdirAll(*outdir, 0755))

	newFq1F, newFq1W := writeFq(
		filepath.Join(
			*outdir,
			strings.Join(
				[]string{
					*name,
					*barcode,
					"_1.fq.gz",
				}, ",",
			),
		),
	)
	defer simple_util.DeferClose(newFq1F)
	defer simple_util.DeferClose(newFq1W)

	newFq2F, newFq2W := writeFq(
		filepath.Join(
			*outdir,
			strings.Join(
				[]string{
					*name,
					*barcode,
					"_2.fq.gz",
				}, ",",
			),
		),
	)
	defer simple_util.DeferClose(newFq2F)
	defer simple_util.DeferClose(newFq2W)

	var peNum, hitNum int
	var barcodes [2]string
	var read1, read2 [4]string
	var loop = true
	for loop {
		peNum++
		//for i:=range read1{
		for i := 0; i < 4; i++ {
			loop = fq1S.Scan() && fq2S.Scan()
			if !loop {
				break
			}
			read1[i] = fq1S.Text()
			read2[i] = fq2S.Text()
		}
		if !loop {
			break
		}
		read1Name := strings.Split(read1[0], "/")[0]
		read2Name := strings.Split(read2[0], "/")[0]
		if read1Name != read2Name {
			log.Fatalf("PE:%d[%s!=%s]", peNum, read1Name, read2Name)
		}
		barcodes[0] = read1[1][:7]
		barcodes[1] = read2[1][:7]
		if !barcodeHash[barcodes[0]] || !barcodeHash[barcodes[1]] {
			continue
		}
		hitNum++
		read1[1] = read1[1][8:]
		read2[1] = read2[1][8:]
		read1[3] = read1[3][8:]
		read2[3] = read2[3][8:]
		_, err = fmt.Fprintln(newFq1W, strings.Join(read1[:], "\n"))
		simple_util.CheckErr(err)
		_, err = fmt.Fprintln(newFq2W, strings.Join(read2[:], "\n"))
		simple_util.CheckErr(err)
	}
	simple_util.CheckErr(fq1S.Err())
	simple_util.CheckErr(fq2S.Err())
	log.Printf("%s\t%d\t%s\t%d", *barcode, peNum, *name, hitNum)
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
