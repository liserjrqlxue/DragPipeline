#!/usr/bin/env bash
workdir=$1
pipeline=$2
sampleID=$3

Workdir=$workdir/$sampleID/bwa
export PATH=$pipeline/tools:$PATH

echo `date` Start MarkDuplicates
gatk \
    FixMateInformation \
    -I $Workdir/$sampleID.sort.bam \
    -O $Workdir/$sampleID.fix.bam \
    --showHidden \
&&echo `date` Done
