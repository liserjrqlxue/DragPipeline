package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/liserjrqlxue/goUtil/fmtUtil"
	"github.com/liserjrqlxue/goUtil/osUtil"
	"github.com/liserjrqlxue/goUtil/simpleUtil"
)

func createShell(fileName, script string, args ...string) {
	var file = osUtil.Create(fileName)
	defer simpleUtil.DeferClose(file)

	fmtUtil.Fprintf(file, "#!/bin/bash\n#$ -e %s\n#$ -o %s\nsh %s %s\n", filepath.Dir(fileName), filepath.Dir(fileName), script, strings.Join(args, " "))
}

func createDir(workDir string, batchDirList, sampleDirList []string, info Info) {
	for _, subDir := range batchDirList {
		simpleUtil.CheckErr(
			os.MkdirAll(
				filepath.Join(
					workDir,
					subDir,
				),
				0755,
			),
		)
	}
	for sampleID := range info.SampleMap {
		for _, subDir := range sampleDirList {
			simpleUtil.CheckErr(
				os.MkdirAll(
					filepath.Join(
						workDir,
						sampleID,
						subDir,
					),
					0755,
				),
			)
		}
	}
}
