package main

import (
	"github.com/liserjrqlxue/simple-util"
	"log"
	"path/filepath"
	"strings"
	"time"
)

type Task struct {
	TaskName       string
	TaskType       string
	TaskScript     string
	TaskArgs       []string
	TaskInfo       map[string]string
	TaskToChan     map[string]map[string]*chan string
	TaskFrom       []*Task
	First, End     bool
	Scripts        map[string]string
	BatchScript    string
	BarcodeScripts map[string]string
	mem            string
	thread         string
	submitArgs     []string
}

func createStartTask() *Task {
	return &Task{
		TaskName:   "Start",
		TaskToChan: make(map[string]map[string]*chan string),
		First:      true,
		TaskType:   "batch",
	}
}

func createEndTask() *Task {
	return &Task{
		TaskName:   "End",
		TaskToChan: make(map[string]map[string]*chan string),
		End:        true,
		TaskType:   "batch",
	}
}

func createTask(cfg map[string]string, local string, submitArgs []string) *Task {
	task := Task{
		TaskName:       cfg["name"],
		TaskInfo:       cfg,
		TaskType:       cfg["type"],
		TaskScript:     filepath.Join(local, "script", cfg["name"]+".sh"),
		TaskArgs:       strings.Split(cfg["args"], ","),
		TaskToChan:     make(map[string]map[string]*chan string),
		Scripts:        make(map[string]string),
		BarcodeScripts: make(map[string]string),
		mem:            cfg["mem"],
		thread:         cfg["thread"],
		submitArgs:     append(submitArgs, "-l", "vf="+cfg["mem"]+"G,p="+cfg["thread"]),
		End:            true,
	}
	if cfg["submitArgs"] != "" {
		task.submitArgs = append(task.submitArgs, sep.Split(cfg["submitArgs"], -1)...)
	}
	return &task
}

func (task *Task) Start(info Info) {
	for taskName, chanMap := range task.TaskToChan {
		log.Printf("%-7s -> Task[%-7s]", task.TaskName, taskName)
		nextTask := taskList[taskName]
		var router = task.TaskType + "->" + nextTask.TaskType
		switch router {
		case "batch->batch":
			log.Printf("Task[%-7s:%s] -> Task[%-7s:%s]", task.TaskName, "batch", taskName, "batch")
		case "batch->barcode":
			for barcode := range info.BarcodeMap {
				log.Printf("Task[%-7s:%s] -> Task[%-7s:%s]", task.TaskName, "batch", taskName, barcode)

			}
		case "batch->sample":
			for sampleID := range chanMap {
				log.Printf("Task[%-7s:%s] -> Task[%-7s:%s]", task.TaskName, "batch", taskName, sampleID)
			}
		}
		for sampleID := range chanMap {
			ch := chanMap[sampleID]
			go func(ch *chan string) { *ch <- "" }(ch)
		}
	}
}

func (task *Task) WaitEnd(info Info) {
	for _, fromTask := range task.TaskFrom {
		var router = fromTask.TaskType + "->" + task.TaskType
		chanMap := fromTask.TaskToChan[task.TaskName]
		switch router {
		case "batch->batch":
			log.Printf("Task[%-7s:%s] <- %s", task.TaskName, "batch", <-*chanMap["batch"])
		case "barcode->batch":
			for barcode := range info.BarcodeMap {
				log.Printf("Task[%-7s:%s] <- %s", task.TaskName, "batch", <-*chanMap[barcode])
			}
		case "sample->batch":
			for sampleID := range info.SampleMap {
				log.Printf("Task[%-7s:%s] <- %s", task.TaskName, "batch", <-*chanMap[sampleID])
			}
		}
		log.Printf("%-7s <- Task[%-7s]", task.TaskName, fromTask.TaskName)
	}
}

func (task *Task) CreateScripts(info Info) {
	switch task.TaskType {
	case "sample":
		task.createSampleScripts(info)
	case "batch":
		task.createBatchScripts(info)
	case "barcode":
		task.createBarcodeScripts(info)
	}
}

func (task *Task) createSampleScripts(info Info) {
	for sampleID, sampleInfo := range info.SampleMap {
		script := filepath.Join(*outDir, sampleID, "shell", task.TaskName+".sh")
		task.Scripts[sampleID] = script
		var appendArgs []string
		appendArgs = append(appendArgs, *outDir, *localpath, sampleID)
		for _, arg := range task.TaskArgs {
			switch arg {
			default:
				appendArgs = append(appendArgs, sampleInfo.info[arg])
			}
		}
		createShell(script, task.TaskScript, appendArgs...)
	}
}

func (task *Task) createBatchScripts(info Info) {
	script := filepath.Join(*outDir, "shell", task.TaskName+".sh")
	task.BatchScript = script
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
}

func (task *Task) createBarcodeScripts(info Info) {
	for barcode, barcodeInfo := range info.BarcodeMap {
		script := filepath.Join(*outDir, "shell", strings.Join([]string{"barcode", barcode, task.TaskName, "sh"}, "."))
		task.BarcodeScripts[barcode] = script
		var appendArgs []string
		appendArgs = append(appendArgs, *outDir, *localpath)
		for _, arg := range task.TaskArgs {
			switch arg {
			case "barcode":
				appendArgs = append(appendArgs, barcode)
			case "fq1":
				appendArgs = append(appendArgs, barcodeInfo.fq1)
			case "fq2":
				appendArgs = append(appendArgs, barcodeInfo.fq2)
			case "list":
				appendArgs = append(appendArgs, barcodeInfo.list)
			}
		}
		createShell(script, task.TaskScript, appendArgs...)
	}
}

func (task *Task) RunTask(info Info, throttle chan bool) {
	switch task.TaskType {
	case "sample":
		for sampleID := range info.SampleMap {
			go task.RunJob(info, sampleID, throttle)
		}
	case "barcode":
		for barcode := range info.BarcodeMap {
			go task.RunJob(info, barcode, throttle)
		}
	case "batch":
		go task.RunJob(info, "batch", throttle)
	}
}

func (task *Task) RunJob(info Info, jobName string, throttle chan bool) {
	var hjid = task.WaitFrom(info, jobName)
	log.Printf("Task[%-7s:%s] <- {%s}", task.TaskName, jobName, hjid)
	var jid = task.TaskName + "[" + jobName + "]"
	jid = task.RunScript(jobName, hjid, jid, throttle)
	task.SetEnd(info, jid, jobName)
}

func (task *Task) RunScript(jobName, depJID, jid string, throttle chan bool) string {
	var script string
	switch task.TaskType {
	case "sample":
		script = task.Scripts[jobName]
	case "barcode":
		script = task.BarcodeScripts[jobName]
	case "batch":
		script = task.BatchScript
	}
	if simple_util.FileExists(script + ".complete") {
		log.Printf("skip complete script:%s", script)
		return ""
	}
	switch *mode {
	case "sge":
		jid = simple_util.SGEsubmit([]string{script}, depJID, task.submitArgs)
	default:
		throttle <- true
		log.Printf("Run Task[%-7s:%s]:%s", task.TaskName, jobName, script)
		if *dryRun {
			time.Sleep(10 * time.Second)
		} else {
			simple_util.CheckErr(simple_util.RunCmd("bash", script))
		}
		<-throttle
	}
	return jid
}

func (task *Task) WaitFrom(info Info, jobName string) string {
	var hjid = make(map[string]bool)
	for _, fromTask := range task.TaskFrom {
		var router = fromTask.TaskType + "->" + task.TaskType
		switch router {
		case "barcode->batch":
			for barcode := range info.BarcodeMap {
				ch := fromTask.TaskToChan[task.TaskName][barcode]
				jid := <-*ch
				if jid != "" {
					hjid[jid] = true
				}
			}
		case "sample->batch":
			for sampleID := range info.SampleMap {
				ch := fromTask.TaskToChan[task.TaskName][sampleID]
				jid := <-*ch
				if jid != "" {
					hjid[jid] = true
				}
			}
		case "sample->barcode":
			for sampleID := range info.BarcodeMap[jobName].samples {
				ch := fromTask.TaskToChan[task.TaskName][sampleID]
				jid := <-*ch
				if jid != "" {
					hjid[jid] = true
				}
			}
		case "batch->batch", "batch->barcode", "barcode->barcode", "batch->sample", "barcode->sample", "sample->sample":
			ch := fromTask.TaskToChan[task.TaskName][jobName]
			jid := <-*ch
			if jid != "" {
				hjid[jid] = true
			}
		default:
			log.Fatal("not support task type router:", router)
		}
	}
	var jid []string
	for id := range hjid {
		jid = append(jid, id)
	}
	return strings.Join(jid, ",")
}

func (task *Task) SetEnd(info Info, jid, jobName string) {
	for nextTask, chanMap := range task.TaskToChan {
		_, ok := taskList[nextTask]
		if !ok {
			log.Fatalf("can not find nextTask:%s of task:%+v", nextTask, task)
		}
		var router = task.TaskType + "->" + taskList[nextTask].TaskType
		switch router {
		case "batch->batch":
			log.Printf("Task[%-7s:%s] -> {%s} -> Task[%-7s:%s]", task.TaskName, jobName, jid, nextTask, jobName)
			*chanMap[jobName] <- jid
		case "batch->barcode":
			for barcode := range info.BarcodeMap {
				log.Printf("Task[%-7s:%s] -> {%s} -> Task[%-7s:%s]", task.TaskName, jobName, jid, nextTask, barcode)
				*chanMap[barcode] <- jid
			}
		case "batch->sample":
			for sampleID := range info.SampleMap {
				log.Printf("Task[%-7s:%s] -> {%s} -> Task[%-7s:%s]", task.TaskName, jobName, jid, nextTask, sampleID)
				*chanMap[sampleID] <- jid
			}
		case "barcode->batch":
			log.Printf("Task[%-7s:%s] -> {%s} -> Task[%-7s:%s]", task.TaskName, jobName, jid, nextTask, "batch")
			*chanMap[jobName] <- jid
		case "barcode->barcode":
			log.Printf("Task[%-7s:%s] -> {%s} -> Task[%-7s:%s]", task.TaskName, jobName, jid, nextTask, jobName)
			*chanMap[jobName] <- jid
		case "barcode->sample":
			for sampleID := range info.BarcodeMap[jobName].samples {
				log.Printf("Task[%-7s:%s] -> {%s} -> Task[%-7s:%s]", task.TaskName, jobName, jid, nextTask, sampleID)
				*chanMap[sampleID] <- jid
			}
		case "sample->batch":
			log.Printf("Task[%-7s:%s] -> {%s} -> Task[%-7s:%s]", task.TaskName, jobName, jid, nextTask, "batch")
			*chanMap[jobName] <- jid
		case "sample->barcode":
			log.Printf("Task[%-7s:%s] -> {%s} -> Task[%-7s:%s]", task.TaskName, jobName, jid, nextTask, info.SampleMap[jobName].barcode)
			*chanMap[jobName] <- jid
		case "sample->sample":
			log.Printf("Task[%-7s:%s] -> {%s} -> Task[%-7s:%s]", task.TaskName, jobName, jid, nextTask, jobName)
			*chanMap[jobName] <- jid
		}
	}
}
