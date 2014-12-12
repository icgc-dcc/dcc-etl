#!/bin/bash

# This script builds a gene database used by snpEff for mutation predictions
#
# A new gene database should be prepared when:
# - snpEff was upgraded, or
# - the gene model was upgraded, or
# - the reference genome was upgraded
#
# Prerequisites:
#   1. Download snpEff tool. http://snpeff.sourceforge.net/download.html
#   2. Download reference genome annotations. E.g. ftp://ftp.ensembl.org/pub/release-75/gtf/homo_sapiens/Homo_sapiens.GRCh37.75.gtf.gz
#   3. Download sequence files. E.g. https://seqwaremaven.oicr.on.ca/artifactory/simple/dcc-dependencies/org/icgc/dcc/dcc-reference-genome/GRCh37.75.v1/dcc-reference-genome-GRCh37.75.v1.tar.gz

#
# Global variables
#

# Location of SnpEff archive. (Extracted to snpEff)
SNP_URI=$HOME/tmp/snpEff-3.6/snpEff_v3_6_core.zip
GTF_URI="$HOME/dcc/GeneDatabase/Homo_sapiens.GRCh37.75.gtf.gz"
FASTA_URI="$HOME/tmp/anno/GRCh37.75.v1.fasta"

# Database name being built
DCC_DB=GRCh37.75.dcc

WORK_DIR="$HOME/tmp/db"
SNP_DIR=$WORK_DIR/snpEff
BUILD_LOG=$SNP_DIR/build.log
SNP_DATA_DIR=$SNP_DIR/data
DCC_DB_DIR=$SNP_DATA_DIR/$DCC_DB

#
# Functions
#

# Resolves reference genome sequence files
resolve_fasta() {
  cd $DCC_DB_DIR
  ln -s $FASTA_URI sequences.fa
}

# Resolve genome annotations
resolve_gtf() { 
  cp $GTF_URI $DCC_DB_DIR/genes.gtf.gz
}

#
# Main()
#
echo Starting parameters
echo Working dir: $WORK_DIR
echo DB Name: $DCC_DB

# Prepare working directory
if [ -d $WORK_DIR ]; then
  echo Working dir exists. Removing. 
  rm -rf $WORK_DIR
fi
mkdir -p $WORK_DIR

# Configure snpEff tool
cp $SNP_URI $WORK_DIR
cd $WORK_DIR
unzip $(basename $SNP_URI) > /dev/null

# Prepare snpEff.config
echo "$DCC_DB.genome : Homo_sapiens" >> $SNP_DIR/snpEff.config
echo "$DCC_DB.MT.codonTable : Vertebrate_Mitochondrial" >> $SNP_DIR/snpEff.config

mkdir -p $DCC_DB_DIR
cd $DCC_DB_DIR
resolve_gtf
resolve_fasta

# Run
echo Starting Database build.
echo Output will be redirected to $BUILD_LOG
cd $SNP_DIR
nice java -Xmx4g -jar snpEff.jar build -gtf22 -v $DCC_DB > $BUILD_LOG 2>&1
echo Done building the Database
