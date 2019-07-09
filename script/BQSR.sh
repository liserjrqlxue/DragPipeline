#!/usr/bin/env bash
workdir=$1
pipeline=$2
sampleID=$3

Workdir=$workdir/$sampleID/bwa
export PATH=$pipeline/tools:$PATH
Bed=$pipeline/etc/target.bed
DbSNP=$pipeline/hg19/dbsnp_138.hg19.vcf
GoldIndels=$pipeline/hg19/Mills_and_1000G_gold_standard.indels.hg19.sites.vcf.gz
hg19=$pipeline/hg19/hg19_chM_male_mask.fa

mkdir -p $workdir/javatmp
echo `date` Start BQSR
gatk \
    BaseRecalibrator \
    --tmp-dir=$workdir/javatmp \
    -I $Workdir/$sampleID.realn.bam \
    -O $Workdir/$sampleID.recal_data.grp \
    --known-sites $DbSNP \
    --known-sites $GoldIndels \
    -R $hg19 \
    -L $Bed \
    --showHidden \
&&echo `date` Done
