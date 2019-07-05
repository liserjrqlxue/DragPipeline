package main

import (
	"flag"
	"github.com/liserjrqlxue/simple-util"
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
		filepath.Join(exPath, "test", "input.list"),
		"input list",
	)
	workdir = flag.String(
		"workdir",
		"workdir",
		"workdir",
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
)

var sampleDirList = []string{
	"raw",
	"filter",
	"bwa",
	"shell",
}

var (
	sep = regexp.MustCompile(`\s+`)
)

func main() {
	flag.Parse()
	if *input == "" {
		flag.Usage()
		os.Exit(0)
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

	inputInfo, _ := simple_util.File2MapArray(*input, "\t", nil)
	var SampleInfo = make(map[string]map[string]string)
	var sampleList []string
	for _, item := range inputInfo {
		sampleID := item["sampleID"]
		sampleList = append(sampleList, sampleID)
		SampleInfo[sampleID] = item
	}
	createDir(*workdir, sampleDirList, sampleList)

	// create taskList
	cfgInfo, _ := simple_util.File2MapArray(*cfg, "\t", nil)
	var taskList = make(map[string]*Task)
	for _, item := range cfgInfo {
		task := createTask(item, *localpath, submitArgs)
		taskList[task.TaskName] = task
		// create scripts
		task.CreateScripts(SampleInfo)
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
				for sampleID := range SampleInfo {
					ch := make(chan string)
					sampleListChan[sampleID] = &ch
				}
				fromTask.TaskToChan[taskName] = sampleListChan
			}
		} else {
			item.TaskFrom = append(item.TaskFrom, startTask)

			sampleListChan := make(map[string]*chan string)
			for sampleID := range SampleInfo {
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
			for sampleID := range SampleInfo {
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
			for sampleID := range SampleInfo {
				go task.RunTask(sampleID)
			}
		}
	}

	// start run
	startTask.Start()
	// wait finish
	endTask.WaitEnd()
}
