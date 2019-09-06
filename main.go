package main

import (
	"flag"
	"github.com/liserjrqlxue/simple-util"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// os
var (
	ex, _  = os.Executable()
	exPath = filepath.Dir(ex)
)

var (
	input = flag.String(
		"input",
		"",
		"input list",
	)
	outDir = flag.String(
		"outdir",
		"",
		"output dir",
	)
	localpath = flag.String(
		"local",
		exPath,
		"local path",
	)
	cfg = flag.String(
		"cfg",
		filepath.Join(exPath, "etc", "allSteps.tsv"),
		"pipeline",
	)
	mode = flag.String(
		"mode",
		"local",
		"run mode:[local|sge]",
	)
	cwd = flag.Bool(
		"cwd",
		false,
		"add -cwd for SGE",
	)
	proj = flag.String(
		"P",
		"",
		"project for SGE(-P)",
	)
	queue = flag.String(
		"q",
		"",
		"queue for SGE(-q)",
	)
	threshold = flag.Int(
		"threshold",
		12,
		"threshold limit for local mode",
	)
	logFile = flag.String(
		"log",
		"",
		"output log file",
	)
	dryRun = flag.Bool(
		"dryRun",
		false,
		"dry run for local",
	)
)

var batchDirList = []string{
	"shell",
	"javatmp",
}

var sampleDirList = []string{
	"raw",
	"filter",
	"bwa",
	"shell",
	"vcf",
}

var (
	sep = regexp.MustCompile(`\s+`)
)

type Info struct {
	Samples    []string
	SampleMap  map[string]*Sample
	BarcodeMap map[string]*Barcode
}

type Barcode struct {
	barcode    string
	list       string
	fq1        string
	fq2        string
	samples    map[string]*Sample
	sampleList []string
}

type Sample struct {
	sampleID  string
	sampleNum string
	barcode   string
	primer    string
	info      map[string]string
}

func parseInput(input, outDir string) (info Info) {
	info = Info{
		Samples:    []string{},
		SampleMap:  make(map[string]*Sample),
		BarcodeMap: make(map[string]*Barcode),
	}
	inputInfo, _ := simple_util.File2MapArray(input, "\t", nil)
	for _, item := range inputInfo {
		sampleID := item["sampleID"]
		barcode := item["barcode"]
		info.Samples = append(info.Samples, sampleID)

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

	for _, barcodeInfo := range info.BarcodeMap {
		var sampleList []string
		for sampleID := range barcodeInfo.samples {
			sampleList = append(sampleList, sampleID)
		}
		barcodeInfo.sampleList = sampleList
	}
	return
}

var throttle chan bool

func main() {
	flag.Parse()
	if *input == "" || *outDir == "" {
		flag.Usage()
		log.Printf("-input and -outdir required")
		os.Exit(0)
	}

	log.SetFlags(log.Ldate | log.Ltime)
	if *logFile != "" {
		//*logFile = filepath.Join(*outDir, "log")
		simple_util.CheckErr(os.MkdirAll(filepath.Dir(*logFile), 0755))
		logF, err := os.Create(*logFile)
		simple_util.CheckErr(err)
		defer simple_util.DeferClose(logF)
		log.SetOutput(logF)
		log.Printf("Log file:%v\n", *logFile)
	}

	throttle = make(chan bool, *threshold)

	var submitArgs []string
	if *cwd {
		submitArgs = append(submitArgs, "-cwd")
	}
	if *queue != "" {
		submitArgs = append(submitArgs, "-q", *queue)
	}
	if *proj != "" {
		submitArgs = append(submitArgs, "-P", *proj)
	}
	info := parseInput(*input, *outDir)
	createDir(*outDir, batchDirList, sampleDirList, info)
	simple_util.CheckErr(simple_util.CopyFile(filepath.Join(*outDir, "input.list"), *input))

	// create taskList
	cfgInfo, _ := simple_util.File2MapArray(*cfg, "\t", nil)
	var taskList = make(map[string]*Task)
	for _, item := range cfgInfo {
		task := createTask(item, *localpath, submitArgs)
		taskList[task.TaskName] = task
		// create scripts
		task.CreateScripts(info)
	}
	var startTask = createStartTask()
	var endTask = createEndTask()
	// add prior to current TaskFrom and add current task to prior's TaskToChan
	// set startTask as prior of first tasks
	for taskName, item := range taskList {
		prior := item.TaskInfo["prior"]
		if prior != "" {
			for _, from := range strings.Split(prior, ",") {
				fromTask := taskList[from]
				item.TaskFrom = append(item.TaskFrom, fromTask)
				fromTask.End = false

				sampleListChan := make(map[string]*chan string)
				for sampleID := range info.SampleMap {
					ch := make(chan string)
					sampleListChan[sampleID] = &ch
				}
				fromTask.TaskToChan[taskName] = sampleListChan
			}
		} else {
			item.TaskFrom = append(item.TaskFrom, startTask)

			sampleListChan := make(map[string]*chan string)
			for sampleID := range info.SampleMap {
				ch := make(chan string)
				sampleListChan[sampleID] = &ch
			}
			startTask.TaskToChan[taskName] = sampleListChan
		}
	}
	// set end task as prior of endTask
	for _, item := range taskList {
		if item.End {
			endTask.TaskFrom = append(endTask.TaskFrom, item)
			item.End = false

			sampleListChan := make(map[string]*chan string)
			for sampleID := range info.SampleMap {
				ch := make(chan string)
				sampleListChan[sampleID] = &ch
			}
			item.TaskToChan[endTask.TaskName] = sampleListChan
		}
	}

	// runTask
	for _, task := range taskList {
		switch task.TaskType {
		case "sample":
			for sampleID := range info.SampleMap {
				//go task.RunSampleTask(sampleID)
				go task.RunTask(info, sampleID, task.Scripts[sampleID], []string{sampleID})
			}
		case "batch":
			//go task.RunBatchTask(info)
			go task.RunTask(info, "batch", task.BatchScript, info.Samples)
		case "barcode":
			for barcode, barcodeInfo := range info.BarcodeMap {
				//go task.RunBarcodeTask(barcode,info)
				var sampleList []string
				for sampleID := range barcodeInfo.samples {
					sampleList = append(sampleList, sampleID)
				}
				go task.RunTask(info, barcode, task.BarcodeScripts[barcode], barcodeInfo.sampleList)
			}
		}
	}

	// start run
	startTask.Start()
	// wait finish
	endTask.WaitEnd()

	for i := 0; i < *threshold; i++ {
		throttle <- true
	}
	log.Printf("All Done!")
}
