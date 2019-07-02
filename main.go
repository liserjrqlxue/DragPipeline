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
	TaskInfo map[string]string
	TaskFrom []*Task
	TaskTo   []*Task
	ChanFrom map[string]*chan string
	ChanTo   map[string]*chan string
	Script   string
}

func main() {
	flag.Parse()
	if *input == "" {
		flag.Usage()
		os.Exit(0)
	}

	inputInfo, _ := simple_util.File2MapArray(*input, "\t", nil)
	os.MkdirAll(*workdir, 0755)
	for _, item := range inputInfo {
		sampleID := item["sampeID"]
		os.MkdirAll(filepath.Join(*workdir, sampleID), 0755)
	}
	var sampleInfo = inputInfo[0]
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
		name := item["name"]
		task := Task{
			TaskName: name,
			TaskInfo: item,
			Script:   filepath.Join(*workdir, sampleID, item["name"]+".sh"),
		}
		taskList[name] = &task
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
			log.Printf("Run task[%s],from[%+v]", taskName, froms)
			var jid = taskName
			switch *mode {
			case "sge":
				oldChan := make(chan string)
				oldChan <- strings.Join(froms, ",")
				newChan := make(chan string)
				simple_util.SGEsubmmit(i, []string{item.Script}, oldChan, newChan, nil)
				jid = <-newChan
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
