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
	Sample  map[string]map[string]string // sample -> barcode
	Barcode map[string][]string          // barcode -> samples
	Samples []string
}

func parseInput(input string) (info Info) {
	info = Info{
		Sample:  make(map[string]map[string]string),
		Barcode: make(map[string][]string),
		Samples: []string{},
	}
	inputInfo, _ := simple_util.File2MapArray(input, "\t", nil)
	for _, item := range inputInfo {
		sampleID := item["sampleID"]
		barcode := item["barcode"]
		info.Sample[sampleID] = item
		samples := info.Barcode[barcode]
		samples = append(samples, sampleID)
		info.Barcode[barcode] = samples
		info.Samples = append(info.Samples, sampleID)
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

	if *logFile == "" {
		*logFile = filepath.Join(*outDir, "log")
	}
	logF, err := os.Create(*logFile)
	simple_util.CheckErr(err)
	defer simple_util.DeferClose(logF)
	log.SetOutput(logF)
	log.SetFlags(log.Ldate | log.Ltime)
	log.Printf("Log file:%v\n", *logFile)

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
	info := parseInput(*input)
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
				for sampleID := range info.Sample {
					ch := make(chan string)
					sampleListChan[sampleID] = &ch
				}
				fromTask.TaskToChan[taskName] = sampleListChan
			}
		} else {
			item.TaskFrom = append(item.TaskFrom, startTask)

			sampleListChan := make(map[string]*chan string)
			for sampleID := range info.Sample {
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
			for sampleID := range info.Sample {
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
			for sampleID := range info.Sample {
				go task.RunTask(sampleID)
			}
		case "batch":
			go task.RunBatchTask(info)
		}
	}

	// start run
	startTask.Start()
	// wait finish
	endTask.WaitEnd()

	for i := 0; i < *threshold; i++ {
		throttle <- true
	}
}
