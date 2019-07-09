#!/usr/bin/env bash
workdir=$1
pipeline=$2
sampleID=$3

Workdir=$workdir/$sampleID/bwa
export PATH=$pipeline/tools:$PATH

echo `date` Start SortSam
gatk \
  SortSam \
  -I $Workdir/$sampleID.raw.bam \
  -O $Workdir/$sampleID.sort.bam \
  -SO coordinate \
  --showHidden=true

echo `date` Done
