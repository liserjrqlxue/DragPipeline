package main

import (
	"github.com/liserjrqlxue/simple-util"
	"log"
	"path/filepath"
	"strings"
)

type Task struct {
	TaskName   string
	TaskType   string
	TaskScript string
	TaskArgs   []string
	TaskInfo   map[string]string
	// toTask sample chan
	TaskToChan map[string]map[string]*chan string
	TaskFrom   []*Task
	First, End bool
	Scripts    map[string]string
	mem        string
	thread     string
	submitArgs []string
}

func createStartTask() *Task {
	return &Task{
		TaskName:   "Start",
		TaskToChan: make(map[string]map[string]*chan string),
		First:      true,
	}
}

func createEndTask() *Task {
	return &Task{
		TaskName:   "End",
		TaskToChan: make(map[string]map[string]*chan string),
		End:        true,
	}
}

func createTask(cfg map[string]string, local string, submitArgs []string) *Task {
	task := Task{
		TaskName:   cfg["name"],
		TaskInfo:   cfg,
		TaskType:   cfg["type"],
		TaskScript: filepath.Join(local, "script", cfg["name"]+".sh"),
		TaskArgs:   strings.Split(cfg["args"], ","),
		TaskToChan: make(map[string]map[string]*chan string),
		Scripts:    make(map[string]string),
		mem:        cfg["mem"],
		thread:     cfg["thread"],
		submitArgs: append(submitArgs, "-l", "vf="+cfg["mem"]+"G,p="+cfg["thread"], cfg["submitArgs"]),
		End:        true,
	}
	if cfg["submitArgs"] != "" {
		task.submitArgs = append(task.submitArgs, sep.Split(cfg["submitArgs"], -1)...)
	}
	return &task
}

func (task *Task) Start() {
	for taskName, chanMap := range task.TaskToChan {
		log.Printf("%-7s -> Task[%-7s]", task.TaskName, taskName)
		for sampleID := range chanMap {
			ch := chanMap[sampleID]
			log.Printf("Task[%-7s:%s] -> Task[%-7s:%s]", task.TaskName, sampleID, taskName, sampleID)
			*ch <- ""
		}
	}
}

func (task *Task) WaitEnd() {
	for _, fromTask := range task.TaskFrom {
		for taskName, chanMap := range fromTask.TaskToChan {
			for sampleID := range chanMap {
				log.Printf("Task[%-7s:%s] <- %s", taskName, sampleID, <-*chanMap[sampleID])
			}
		}
		log.Printf("%-7s <- Task[%-7s]", task.TaskName, fromTask.TaskName)
	}
}

func (task *Task) RunTask(sampleID string) {
	var froms []string
	for _, fromTask := range task.TaskFrom {
		ch := fromTask.TaskToChan[task.TaskName][sampleID]
		fromInfo := <-*ch
		froms = append(froms, fromInfo)
	}
	var jid = task.TaskName + ":" + sampleID
	log.Printf("Task[%-7s:%s] <- {%s}", task.TaskName, sampleID, strings.Join(froms, ","))
	switch *mode {
	case "sge":
		var hjid = strings.Join(froms, ",")
		jid = simple_util.SGEsubmit([]string{task.Scripts[sampleID]}, hjid, task.submitArgs)
	default:
		log.Printf("Run Task[%-7s:%s]:%s", task.TaskName, sampleID, task.Scripts[sampleID])
		simple_util.CheckErr(simple_util.RunCmd("bash", task.Scripts[sampleID]))
	}
	for _, chanMap := range task.TaskToChan {
		log.Printf("Task[%-7s:%s] -> %s", task.TaskName, sampleID, jid)
		*chanMap[sampleID] <- jid
	}
}

func (task *Task) CreateScripts(SampleInfo map[string]map[string]string) {
	switch task.TaskType {
	case "sample":
		task.createSampleScripts(SampleInfo)
	}
}

func (task *Task) createSampleScripts(SampleInfo map[string]map[string]string) {
	for sampleID, sampleInfo := range SampleInfo {
		script := filepath.Join(*workdir, sampleID, "shell", task.TaskName+".sh")
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
