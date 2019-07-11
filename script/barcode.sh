#!/usr/bin/env bash
echo $0 $@
workdir=$1
pipeline=$2
list=$3
sampleID=$3
fq1=$4
fq2=$5
barcode=$6
p1=$7
p2=$8

Workdir=$workdir/$sampleID/raw
export PATH=$pipeline/tools:$PATH

echo `date` Start splitBarcode
$pipeline/splitBarcode/splitBarcode \
    -input $list \
    -outdir $workdir \
&&echo Done `date`
