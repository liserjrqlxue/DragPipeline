#!/usr/bin/env bash
workdir=$1
pipeline=$2
sampleID=$3

Workdir=$workdir/$sampleID
export PATH=$pipeline/tools:$PATH

fq1=$Workdir/raw/${sampleID}.raw_1.fq.gz
fq2=$Workdir/raw/${sampleID}.raw_2.fq.gz

echo `date` Start FastqQC
java \
    -jar $pipeline/tools/FastqQC.jar \
    -o $Workdir/filter \
    -l 10 -q 0.1 -N 0.05 \
    -1 $fq1 -2 $fq2 \
    -C ${sampleID}.filter_1.fq.gz \
    -D ${sampleID}.filter_2.fq.gz \
    >$Workdir/filter/${sampleID}.reads_gc_qual.stat \
&&echo Done `date`
