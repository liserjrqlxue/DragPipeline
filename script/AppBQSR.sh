#!/usr/bin/env bash
workdir=$1
pipeline=$2
sampleID=$3

Workdir=$workdir/$sampleID/bwa
export PATH=$pipeline/tools:$PATH

hg19=$pipeline/hg19/hg19_chM_male_mask.fa

echo `date` Start ApplyBQSR

gatk \
    ApplyBQSR \
    --tmp-dir=$workdir/javatmp \
    -R $hg19 \
    -I $Workdir/$sampleID.realn.bam \
    -O $Workdir/$sampleID.bqsr.bam \
    -bqsr $Workdir/$sampleID.recal_data.grp \
&&echo `date` Done
