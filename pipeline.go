package main

import (
	"fmt"
	"github.com/liserjrqlxue/simple-util"
	"os"
	"strings"
)

func createShell(fileName, script string, args ...string) {
	file, err := os.Create(fileName)
	simple_util.CheckErr(err)
	defer simple_util.DeferClose(file)

	_, err = fmt.Fprintf(file, "#!/bin/bash\nsh %s %s\n", script, strings.Join(args, " "))
	simple_util.CheckErr(err)
}
