#!/usr/bin/env bash
workdir=$1
pipeline=$2
sampleID=$3

Workdir=$workdir/$sampleID/bwa
export PATH=$pipeline/tools:$PATH

echo `date` Start IndexFixBam
samtools index $Workdir/$sampleID.fix.bam

echo `date` Done
