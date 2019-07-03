package main

import (
	"flag"
	"github.com/liserjrqlxue/simple-util"
	"log"
	"os"
	"path/filepath"
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
		filepath.Join(exPath, "..", "WESIM", "pipeline"),
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
)

type Task struct {
	TaskName string
	TaskType string
	TaskArgs []string
	TaskInfo map[string]string
	TaskFrom []*Task
	TaskTo   []*Task
	ChanFrom map[string]*chan string
	ChanTo   map[string]*chan string
	Script   string
}

var sampleDirList = []string{
	"raw",
	"filter",
	"bwa",
	"shell",
}
var laneDirList = []string{
	"filter",
}

func main() {
	flag.Parse()
	if *input == "" {
		flag.Usage()
		os.Exit(0)
	}

	inputInfo, _ := simple_util.File2MapArray(*input, "\t", nil)
	createDir(*workdir, sampleDirList, laneDirList, inputInfo)

	var sampleInfo = inputInfo[0]
	sampleInfo["fq1"] = filepath.Join(sampleInfo["rawDir"], sampleInfo["read1"])
	sampleInfo["fq2"] = filepath.Join(sampleInfo["rawDir"], sampleInfo["read2"])
	var sampleID = sampleInfo["sampleID"]

	cfgInfo, _ := simple_util.File2MapArray(*cfg, "\t", nil)

	var taskList = make(map[string]*Task)
	var startTask = Task{
		TaskName: "Start",
		ChanTo:   make(map[string]*chan string),
	}
	var endTask = Task{
		TaskName: "End",
		ChanFrom: make(map[string]*chan string),
	}

	for _, item := range cfgInfo {
		task := Task{
			TaskName: item["name"],
			TaskInfo: item,
			TaskType: item["type"],
			TaskArgs: strings.Split(item["args"], ","),
			Script:   filepath.Join(*workdir, sampleID, "shell", item["name"]+".sh"),
		}
		taskList[task.TaskName] = &task
		var appendArgs []string
		appendArgs = append(appendArgs, *workdir, *localpath, sampleID)
		switch task.TaskType {
		case "lane":
			for _, arg := range task.TaskArgs {
				switch arg {
				case "laneName":
					appendArgs = append(appendArgs, sampleInfo["lane"])
				case "fq1":
					appendArgs = append(appendArgs, sampleInfo["fq1"])
				case "fq2":
					appendArgs = append(appendArgs, sampleInfo["fq2"])
				default:
					appendArgs = append(appendArgs, sampleInfo[arg])
				}
			}
		case "sample":
			for _, arg := range task.TaskArgs {
				switch arg {
				case "laneName":
					appendArgs = append(appendArgs, sampleInfo["lane"])
				}
			}
		}
		createShell(task.Script, filepath.Join(*localpath, "script", task.TaskName+".sh"), appendArgs...)
	}

	for taskName, item := range taskList {
		prior := item.TaskInfo["prior"]
		chanMap := make(map[string]*chan string)
		if prior != "" {
			for _, from := range strings.Split(prior, ",") {
				item.TaskFrom = append(item.TaskFrom, taskList[from])
				taskList[from].TaskTo = append(taskList[from].TaskTo, item)
				ch := make(chan string)
				chanMap[from] = &ch
			}
		} else {
			item.TaskFrom = append(item.TaskFrom, &startTask)
			ch := make(chan string)
			chanMap["Start"] = &ch
			startTask.ChanTo[taskName] = &ch
		}
		item.ChanFrom = chanMap
	}

	for taskName, item := range taskList {
		if item.TaskTo == nil {
			item.TaskTo = append(item.TaskTo, &endTask)

			ch := make(chan string)
			endTask.ChanFrom[taskName] = &ch
		}
	}

	var i = 1
	// run
	for taskName, item := range taskList {
		go func(i int, taskName string, item *Task) {
			var froms []string
			for _, ch := range item.ChanFrom {
				fromInfo := <-*ch
				froms = append(froms, fromInfo)
			}
			log.Printf("Run task[%s],after%+v:%s", taskName, froms, item.Script)
			var jid = taskName
			switch *mode {
			case "sge":
				hjid := strings.Join(froms, ",")
				jid = simple_util.SGEsubmmit(i, []string{item.Script}, hjid, nil)
			default:
				simple_util.CheckErr(simple_util.RunCmd("bash", item.Script))
			}

			for _, task := range item.TaskTo {
				*task.ChanFrom[taskName] <- jid
			}
		}(i, taskName, item)
	}

	// start goroutine
	for taskName, ch := range startTask.ChanTo {
		log.Printf("Start task[%s}", taskName)
		*ch <- ""
	}
	// wait goroutine end
	for taskName, ch := range endTask.ChanFrom {
		log.Printf("End task[%s]:%s", taskName, <-*ch)
	}
}
