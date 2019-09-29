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

func main() {
	log.Println("args:", os.Args)
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
	// create outDir/step2.sh and write args to it
	simple_util.Array2File(filepath.Join(*outDir, "step2.sh"), " ", os.Args)

	// create taskList
	cfgInfo, _ := simple_util.File2MapArray(*cfg, "\t", nil)
	var taskList = make(map[string]*Task)

	for _, item := range cfgInfo {
		task := createTask(item, *localpath, submitArgs)
		_, ok := taskList[task.TaskName]
		if ok {
			log.Fatal("dup TaskName:", task.TaskName)
		}
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
					ch := make(chan string, 1)
					sampleListChan[sampleID] = &ch
				}
				for barcode := range info.BarcodeMap {
					ch := make(chan string, 1)
					sampleListChan[barcode] = &ch
				}
				ch := make(chan string, 1)
				sampleListChan["batch"] = &ch
				fromTask.TaskToChan[taskName] = sampleListChan
			}
		} else {
			item.TaskFrom = append(item.TaskFrom, startTask)

			sampleListChan := make(map[string]*chan string)
			for sampleID := range info.SampleMap {
				ch := make(chan string, 1)
				sampleListChan[sampleID] = &ch
			}
			for barcode := range info.BarcodeMap {
				ch := make(chan string, 1)
				sampleListChan[barcode] = &ch
			}
			ch := make(chan string, 1)
			sampleListChan["batch"] = &ch
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
				ch := make(chan string, 1)
				sampleListChan[sampleID] = &ch
			}
			for barcode := range info.BarcodeMap {
				ch := make(chan string, 1)
				sampleListChan[barcode] = &ch
			}
			ch := make(chan string, 1)
			sampleListChan["batch"] = &ch
			item.TaskToChan[endTask.TaskName] = sampleListChan
		}
	}
	taskList["End"] = endTask

	throttle := make(chan bool, *threshold)
	// runTask
	for _, task := range taskList {
		if task.End {
			continue
		}
		task.RunTask(info, throttle, taskList)
	}

	// start run
	startTask.Start(info, taskList)
	// wait finish
	endTask.WaitEnd(info)

	for i := 0; i < *threshold; i++ {
		throttle <- true
	}
	log.Printf("All Done!")
}
