package main

import (
	"github.com/liserjrqlxue/simple-util"
	"log"
	"path/filepath"
	"strings"
)

type Task struct {
	TaskName    string
	TaskType    string
	TaskScript  string
	TaskArgs    []string
	TaskInfo    map[string]string
	TaskToChan  map[string]map[string]*chan string
	TaskFrom    []*Task
	First, End  bool
	Scripts     map[string]string
	BatchScript string
	mem         string
	thread      string
	submitArgs  []string
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
		submitArgs: append(submitArgs, "-l", "vf="+cfg["mem"]+"G,p="+cfg["thread"]),
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
			go func(ch *chan string) { *ch <- "" }(ch)
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

func (task *Task) WaitFrom(sampleIDs ...string) string {
	var hjid []string
	for _, fromTask := range task.TaskFrom {
		for _, sampleID := range sampleIDs {
			ch := fromTask.TaskToChan[task.TaskName][sampleID]
			jid := <-*ch
			if jid != "" {
				hjid = append(hjid, jid)
			}
		}
	}
	return strings.Join(hjid, ",")
}

func (task *Task) RunTask(sampleID string) {
	var hjid = task.WaitFrom(sampleID)
	var jid = task.TaskName + "[" + sampleID + "]"
	log.Printf("Task[%-7s:%s] <- {%s}", task.TaskName, sampleID, hjid)
	switch *mode {
	case "sge":
		jid = simple_util.SGEsubmit([]string{task.Scripts[sampleID]}, hjid, task.submitArgs)
	default:
		throttle <- true
		log.Printf("Run Task[%-7s:%s]:%s", task.TaskName, sampleID, task.Scripts[sampleID])
		simple_util.CheckErr(simple_util.RunCmd("bash", task.Scripts[sampleID]))
		//time.Sleep(10*time.Second)
		<-throttle
	}
	for _, chanMap := range task.TaskToChan {
		log.Printf("Task[%-7s:%s] -> {%s}", task.TaskName, sampleID, jid)
		*chanMap[sampleID] <- jid
	}
}

func (task *Task) RunBatchTask(info Info) {
	var hjid = task.WaitFrom(info.Samples...)
	var jid = task.TaskName + "[batch]"
	log.Printf("Task[%-7s:%s] <- {%s}", task.TaskName, "batch", hjid)
	switch *mode {
	case "sge":
		jid = simple_util.SGEsubmit([]string{task.BatchScript}, hjid, task.submitArgs)
	default:
		throttle <- true
		log.Printf("Run Task[%-7s:%s]:%s", task.TaskName, "batch", task.BatchScript)
		simple_util.CheckErr(simple_util.RunCmd("bash", task.BatchScript))
		//time.Sleep(10*time.Second)
		<-throttle
	}
	for _, chanMap := range task.TaskToChan {
		log.Printf("Task[%-7s:%s] -> {%s}", task.TaskName, "batch", jid)
		for _, sampleID := range info.Samples {
			*chanMap[sampleID] <- jid
		}
	}
}

func (task *Task) CreateScripts(info Info) {
	switch task.TaskType {
	case "sample":
		task.createSampleScripts(info)
	case "batch":
		task.createBatchScripts(info)
	}
}

func (task *Task) createSampleScripts(info Info) {
	for sampleID, sampleInfo := range info.Sample {
		script := filepath.Join(*outDir, sampleID, "shell", task.TaskName+".sh")
		task.Scripts[sampleID] = script
		var appendArgs []string
		appendArgs = append(appendArgs, *outDir, *localpath, sampleID)
		for _, arg := range task.TaskArgs {
			switch arg {
			default:
				appendArgs = append(appendArgs, sampleInfo[arg])
			}
		}
		createShell(script, task.TaskScript, appendArgs...)
	}
}

func (task *Task) createBatchScripts(info Info) {
	script := filepath.Join(*outDir, "shell", task.TaskName+".sh")
	var appendArgs []string
	appendArgs = append(appendArgs, *outDir, *localpath)
	for _, arg := range task.TaskArgs {
		switch arg {
		case "list":
			appendArgs = append(appendArgs, filepath.Join(*outDir, "input.list"))
		default:
		}
	}
	createShell(script, task.TaskScript, appendArgs...)
	task.BatchScript = script
}
