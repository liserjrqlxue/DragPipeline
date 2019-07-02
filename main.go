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
		filepath.Join(exPath, "test", "allSteps.tsv"),
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
)

type Task struct {
	TaskName string
	TaskInfo map[string]string
	TaskFrom []*Task
	TaskTo   []*Task
	ChanFrom map[string]*chan string
	ChanTo   map[string]*chan string
}

type TaskList struct {
	Start     map[string]*Task
	End       map[string]*Task
	ChanStart map[string]*chan string
	ChanEnd   map[string]*chan string
}

func main() {
	flag.Parse()
	if *input == "" {
		flag.Usage()
		os.Exit(0)
	}
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
		}
		taskList[name] = &task
	}

	var orginTaskList = TaskList{
		Start:     make(map[string]*Task),
		End:       make(map[string]*Task),
		ChanStart: make(map[string]*chan string),
		ChanEnd:   make(map[string]*chan string),
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
			orginTaskList.Start[taskName] = item
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

	// run
	for taskName, item := range taskList {
		go func(taskName string, item *Task) {
			var froms []string
			for _, ch := range item.ChanFrom {
				fromInfo := <-*ch
				froms = append(froms, fromInfo)
			}
			log.Printf("Run task[%s],from[%+v]", taskName, froms)
			for _, task := range item.TaskTo {
				*task.ChanFrom[taskName] <- taskName + "->" + task.TaskName
			}
		}(taskName, item)
	}

	// start goroutine
	for taskName, ch := range startTask.ChanTo {
		log.Printf("Start task[%s}", taskName)
		*ch <- "start->" + taskName
	}
	// wait goroutine end
	for taskName, ch := range endTask.ChanFrom {
		log.Printf("End task[%s]:%s", taskName, <-*ch)
	}
}
