#!/usr/bin/env bash
workdir=$1
pipeline=$2
sampleID=$3

Workdir=$workdir/$sampleID/bwa
export PATH=$pipeline/tools:$PATH
GATK=$pipeline/tools/GenomeAnalysisTK.jar
Bed=$pipeline/etc/target.bed
hg19=$pipeline/hg19/hg19_chM_male_mask.fa

echo `date` Start RealignerTargetCreator
java  -Djava.io.tmpdir=$workdir/javatmp \
    -jar $GATK \
    -T RealignerTargetCreator \
    -R $hg19 \
    -L $Bed \
    -I $Workdir/$sampleID.fix.bam \
    -o $Workdir/$sampleID.realn_data.intervals \
&&echo `date` Done
