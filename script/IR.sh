#!/usr/bin/env bash
workdir=$1
pipeline=$2
sampleID=$3

Workdir=$workdir/$sampleID/bwa
export PATH=$pipeline/tools:$PATH
GATK=$pipeline/tools/GenomeAnalysisTK.jar
Bed=$pipeline/config/cns_region_hg19_bychr/for500_region.bed
hg19=$pipeline/hg19/hg19_chM_male_mask.fa

echo `date` Start IndelRealigner
java  -Djava.io.tmpdir=$workdir/javatmp \
    -jar $GATK \
    -T IndelRealigner \
    -R $hg19 \
    -filterNoBases \
    -targetIntervals $Workdir/$sampleID.realn_data.intervals \
    -I $Workdir/$sampleID.fix.bam \
    -o $Workdir/$sampleID.realn.bam \
&&echo `date` Done
