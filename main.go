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

type Task struct {
	TaskName   string
	TaskType   string
	TaskScript string
	TaskArgs   []string
	TaskInfo   map[string]string
	TaskToChan map[string]map[string]*chan string // toTask sample chan
	TaskFrom   []*Task
	TaskTo     []*Task
	ChanFrom   map[string]*chan string
	ChanTo     map[string]*chan string
	Scripts    map[string]string
	mem        string
	thread     string
	submitArgs []string
}

var sampleDirList = []string{
	"raw",
	"filter",
	"bwa",
	"shell",
}

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
		item["fq1"] = filepath.Join(item["rawDir"], item["read1"])
		item["fq2"] = filepath.Join(item["rawDir"], item["read2"])
		SampleInfo[sampleID] = item
	}
	createDir(*workdir, sampleDirList, sampleList)

	cfgInfo, _ := simple_util.File2MapArray(*cfg, "\t", nil)

	var taskList = make(map[string]*Task)
	var startTask = Task{
		TaskName:   "Start",
		ChanTo:     make(map[string]*chan string),
		TaskToChan: make(map[string]map[string]*chan string),
	}
	var endTask = Task{
		TaskName:   "End",
		TaskToChan: make(map[string]map[string]*chan string),
		ChanFrom:   make(map[string]*chan string),
	}

	for _, item := range cfgInfo {
		task := Task{
			TaskName:   item["name"],
			TaskInfo:   item,
			TaskType:   item["type"],
			TaskScript: filepath.Join(*localpath, "script", item["name"]+".sh"),
			TaskArgs:   strings.Split(item["args"], ","),
			TaskToChan: make(map[string]map[string]*chan string),
			Scripts:    make(map[string]string),
			mem:        item["mem"],
			thread:     item["thread"],
			submitArgs: append(submitArgs, "-l", "vf="+item["mem"]+"G,p="+item["thread"], item["submitArgs"]),
		}
		taskList[task.TaskName] = &task
		// create scripts
		switch task.TaskType {
		case "sample":
			for sampleID, sampleInfo := range SampleInfo {
				script := filepath.Join(*workdir, sampleID, "shell", item["name"]+".sh")
				task.Scripts[sampleID] = script
				var appendArgs []string
				appendArgs = append(appendArgs, *workdir, *localpath, sampleID)
				for _, arg := range task.TaskArgs {
					switch arg {
					default:
						appendArgs = append(appendArgs, sampleInfo[arg])
					}
				}
				createShell(script, task.TaskScript, appendArgs...)
			}
		}
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

				sampleListChan := make(map[string]*chan string)
				for sampleID := range SampleInfo {
					ch := make(chan string)
					sampleListChan[sampleID] = &ch
				}
				fromTask := taskList[from]
				fromTask.TaskToChan[taskName] = sampleListChan

			}
		} else {
			item.TaskFrom = append(item.TaskFrom, &startTask)
			ch := make(chan string)
			chanMap["Start"] = &ch
			startTask.ChanTo[taskName] = &ch

			sampleListChan := make(map[string]*chan string)
			for sampleID := range SampleInfo {
				ch := make(chan string)
				sampleListChan[sampleID] = &ch
			}
			startTask.TaskToChan[taskName] = sampleListChan
			log.Printf("%+v", item)
			log.Printf("%+v", startTask)
		}
		item.ChanFrom = chanMap
	}

	for taskName, item := range taskList {
		if item.TaskTo == nil {
			item.TaskTo = append(item.TaskTo, &endTask)

			ch := make(chan string)
			endTask.ChanFrom[taskName] = &ch

			endTask.TaskFrom = append(endTask.TaskFrom, item)
			sampleListChan := make(map[string]*chan string)
			for sampleID := range SampleInfo {
				ch := make(chan string)
				sampleListChan[sampleID] = &ch
			}
			item.TaskToChan[endTask.TaskName] = sampleListChan
		}
	}

	var i = 1
	// run
	for taskName, item := range taskList {
		switch item.TaskType {
		case "sample":
			for sampleID := range SampleInfo {
				go func(i int, sampleID, taskName string, item *Task) {
					var froms []string
					for _, fromTask := range item.TaskFrom {
						ch := fromTask.TaskToChan[taskName][sampleID]
						fromInfo := <-*ch
						froms = append(froms, fromInfo)
					}
					var jid = taskName + ":" + sampleID
					log.Printf("Task[%-7s:%s] <- {%s}", taskName, sampleID, strings.Join(froms, ","))
					switch *mode {
					case "sge":
						var hjid = strings.Join(froms, ",")
						jid = simple_util.SGEsubmit(i, []string{item.Scripts[sampleID]}, hjid, item.submitArgs)
					default:
						log.Printf("Run Task[%-7s:%s]:%s", taskName, sampleID, item.Scripts[sampleID])
						simple_util.CheckErr(simple_util.RunCmd("bash", item.Scripts[sampleID]))
					}
					for _, chanMap := range item.TaskToChan {
						log.Printf("Task[%-7s:%s] -> %s", taskName, sampleID, jid)
						*chanMap[sampleID] <- jid
					}
				}(i, sampleID, taskName, item)
			}
		}
	}

	// start goroutine
	for taskName, chanMap := range startTask.TaskToChan {
		log.Printf("%-7s -> Task[%-7s]", startTask.TaskName, taskName)
		for sampleID := range chanMap {
			ch := chanMap[sampleID]
			log.Printf("Task[%-7s:%s] -> Task[%-7s:%s]", startTask.TaskName, sampleID, taskName, sampleID)
			*ch <- ""
		}
	}
	// wait goroutine end
	for _, fromTask := range endTask.TaskFrom {
		for taskName, chanMap := range fromTask.TaskToChan {
			for sampleID := range chanMap {
				log.Printf("Task[%-7s:%s] <- %s", taskName, sampleID, <-*chanMap[sampleID])
			}
		}
		log.Printf("End <- Task[%-7s]", fromTask.TaskName)
	}
}
