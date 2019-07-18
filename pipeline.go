package main

import (
	"fmt"
	"github.com/liserjrqlxue/simple-util"
	"os"
	"path/filepath"
	"strings"
)

func createShell(fileName, script string, args ...string) {
	file, err := os.Create(fileName)
	simple_util.CheckErr(err)
	defer simple_util.DeferClose(file)

	_, err = fmt.Fprintf(file, "#!/bin/bash\n#$ -e %s\n#$ -o %s\nsh %s %s\n", filepath.Dir(fileName), filepath.Dir(fileName), script, strings.Join(args, " "))
	simple_util.CheckErr(err)
}

func createDir(workDir string, batchDirList, sampleDirList []string, info Info) {
	for _, subDir := range batchDirList {
		simple_util.CheckErr(
			os.MkdirAll(
				filepath.Join(
					workDir,
					subDir,
				),
				0755,
			),
		)
	}
	for sampleID := range info.Sample {
		for _, subDir := range sampleDirList {
			simple_util.CheckErr(
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
