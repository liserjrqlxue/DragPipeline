#!/usr/bin/env bash
workdir=$1
pipeline=$2
sampleID=$3
laneName=$4

Workdir=$workdir/$sampleID
export PATH=$pipeline/tools:$PATH

fq1=$Workdir/raw/${sampleID}_${laneName}_1.fq.gz
fq2=$Workdir/raw/${sampleID}_${laneName}_2.fq.gz

echo `date` Start FastqQC
java \
    -jar $pipeline/tools/FastqQC.jar \
    -o $Workdir/filter/$laneName \
    -l 10 -q 0.1 -N 0.05 \
    -1 $fq1 -2 $fq2 \
    -C pe.$laneName.filter_1.fq.gz \
    -D pe.$laneName.filter_2.fq.gz \
    >$Workdir/filter/$laneName/$laneName.reads_gc_qual.stat \
&&echo Done `date`
