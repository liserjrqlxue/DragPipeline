package main

import (
	"fmt"

	"github.com/liserjrqlxue/goUtil/textUtil"
	"github.com/liserjrqlxue/libIM"
)

func NewInfo(item map[string]string) libIM.Info {
	return libIM.Info{
		SampleID: item["sampleID"],
		Fq1:      item["fq1"],
		Fq2:      item["fq2"],
	}
}

func ParseInfoIM(input string) (infoMap map[string]libIM.Info) {
	infoMap = make(map[string]libIM.Info)
	// parser input list
	var sampleMap, _ = textUtil.File2MapArray(input, "\t", nil)

	for _, item := range sampleMap {
		var sampleID = item["sampleID"]
		info, ok := infoMap[sampleID]
		if !ok {
			info = NewInfo(item)
		} else {
			fmt.Printf("Dup sampleID:%s", sampleID)
		}
		infoMap[sampleID] = info
	}
	return
}

func ParseStepCfg(cfg string, infoMap map[string]libIM.Info) (allSteps []*libIM.Step) {
	var stepList, _ = textUtil.File2MapArray(cfg, "\t", nil)
	var stepMap = make(map[string]*libIM.Step)
	for _, item := range stepList {
		var step = libIM.NewStep(item)
		step.CreateJobs(nil, infoMap, nil, *outDir, *localpath)
		if step.JobSh != nil {
			stepMap[step.Name] = &step
			allSteps = append(allSteps, &step)
		}
	}
	libIM.LinkSteps(stepMap)
	stepMap["first"].First = 1
	return
}
