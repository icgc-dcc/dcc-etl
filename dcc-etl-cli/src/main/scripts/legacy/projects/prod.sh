#!/bin/bash -e

export run_type="prod"
export config_file="***REMOVED***/dcc-etl/conf/etl_prod.yaml"

unset input_projects && declare -A input_projects
input_projects[14]="ALL-US BOCA-UK BRCA-UK EOPC-DE ESAD-UK LAML-KR LINC-JP LIRI-JP MALY-DE ORCA-IN PACA-AU PACA-CA PBCA-DE" # PBCA-DE is by far the biggest (12GB)
input_projects[13]="LICA-FR CESC-US PAAD-US LIHC-US READ-US KIRP-US STAD-US GBM-US LAML-US LGG-US PRAD-US SKCM-US LUSC-US COAD-US LUAD-US HNSC-US UCEC-US THCA-US KIRC-US BLCA-US BRCA-US"
input_projects[13_ambiguous]="OV-US" # OV-US is by far the biggest from ICGC13
input_projects[12]="CLLE-ES" # CLLE-ES is quite big
input_projects[11]="PRAD-UK PRAD-CA"
input_projects[10]="CMDI-UK"
input_projects[9]="PEME-CA"
input_projects[8]="GACA-CN"
export input_projects

export expected_count=41
