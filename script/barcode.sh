#!/usr/bin/env bash
echo $0 $@
workdir=$1
pipeline=$2
sampleID=$3
fq1=$4
fq2=$5
barcode=$6
p1=$7
p2=$8

Workdir=$workdir/$sampleID
export PATH=$pipeline/tools:$PATH

echo `date` Start splitBarcode
echo splitBarcode \
    -barcode $barcode \
    -fq1 $fq1 \
    -fq2 $fq2 \
    -name $sampleID \
    -p1 $p1 \
    -p2 $p2 \
    -outdir $Workdir/raw

perl split_fq_by_indexSeq_pair.pl -o $workdir -b $barcode -s $sampleID -p1 $p1 -p2 $p2 -fq1 $fq1 -fq2 $fq2 \
&&echo Done `date`
