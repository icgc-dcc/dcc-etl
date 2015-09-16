if [ "${type?}" == "ssm" ]; then
 project_ids="ALL-US CMDI-UK"
fi
if [ "${type?}" == "cnsm" ]; then
 project_ids="PBCA-DE EOPC-DE"
fi
if [ "${type?}" == "stsm" ]; then
 project_ids="PRAD-UK PEME-CA"
fi
if [ "${type?}" == "mirna" ]; then
 project_ids="PAAD-US MALY-DE"
fi
if [ "${type?}" == "meth" ]; then
 project_ids="PAAD-US OV-US"
fi
if [ "${type?}" == "exp" ]; then
 project_ids="KIRP-US LGG-US"
fi
if [ "${type?}" == "pexp" ]; then
 project_ids="BLCA-US READ-US"
fi
if [ "${type?}" == "jcn" ]; then
 project_ids="PACA-AU PACA-CA" # same anyway
fi
