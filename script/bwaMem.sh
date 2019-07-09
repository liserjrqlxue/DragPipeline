#!/usr/bin/env bash
workdir=$1
pipeline=$2
sampleID=$3

Workdir=$workdir/$sampleID
export PATH=$pipeline/tools:$PATH
hg19=$pipeline/hg19/hg19_chM_male_mask.fa
echo `date` Start bwaMem
bwa \
    mem -K 1000000 -t 8 -M \
    -R "@RG\tID:$sampleID\tSM:$sampleID\tLB:LB\tPL:COMPLETE" \
    $hg19 \
    $Workdir/filter/$sampleID.filter_1.fq.gz \
    $Workdir/filter/$sampleID.filter_2.fq.gz \
    | samtools view -S -b \
    -o $Workdir/bwa/$sampleID.raw.bam \
    - \
&&echo `date` Done
