#!/bin/bash
#
# Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
#
# This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
# SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
# TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
# IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# Description:
#   internal use for the other scripts for setting the environment
#

# Prolog
set -o nounset
set -o errexit

EXPORTHOMEDIR=`dirname $0`/..; export EXPORTHOMEDIR

JOB_USER=downloader
E_BADARGS=65

if [[ $# -ne 2 && $# -ne 3 ]]
then
  echo "Usage: `basename $0` <release name> <Directory containing json directories> [<comma separated list of data types>]"
  echo "Example: `basename $0` r12 /icgc/etl/dcc-release-r--prod-06d-23-1 # process all data types"
  echo "Example: `basename $0` r12 /icgc/etl/dcc-release-r--prod-06d-23-1 ssm,meth # process ssm and meth only"
  exit $E_BADARGS
fi

release=$1
source=$2

declare -A typeMappings
typeMappings=(["ssm"]="ssm_open,ssm_controlled" 
              ["ssm_open"]="ssm_open" 
              ["ssm_controlled"]="ssm_controlled" 

              ["donor_only"]="donor" 
              ["specimen"]="specimen" 
              ["sample"]="sample" 
              ["donor_family"]="donor_family" 
              ["donor_exposure"]="donor_exposure" 
              ["donor_therapy"]="donor_therapy" 

              ["donor"]="donor,specimen,sample,donor_family,donor_exposure,donor_therapy" 
              ["clinical"]="clinical" 
              ["clinicalsample"]="clinicalsample" 
              ["sgv"]="sgv_controlled" 
              ["sgv_controlled"]="sgv_controlled" 
              ["pexp"]="pexp" 
              ["mirna_seq"]="mirna_seq" 
              ["meth_seq"]="meth_seq"
			  ["meth_array"]="meth_array" 
              ["jcn"]="jcn" 
              ["exp_seq"]="exp_seq"
              ["exp_array"]="exp_array"  
              ["cnsm"]="cnsm" 
              ["stsm"]="stsm")

declare -a datatypes="ssm_open,ssm_controlled,sgv_controlled,pexp,mirna_seq,meth_seq,meth_array,jcn,exp_seq,exp_array,clinical,clinicalsample,cnsm,stsm,donor,specimen,sample,donor_family,donor_exposure,donor_therapy"

if [ $# -eq 3 ]
then
  datatypes=$3
fi

logfile=${EXPORTHOMEDIR}/logs/exporter.ec

IFS=',' read -a types <<< "$datatypes"

datatypes=""
for type in "${types[@]}"
do
	datatypes="${datatypes},${typeMappings["${type//[[:blank:]]/}"]}"
done

datatypes=${datatypes:1}

IFS=',' read -a types <<< "$datatypes"

#export HBASE_HOME=/usr/lib/hbase
export HADOOP_USER_NAME=${JOB_USER}
#export HADOOP_CLASSPATH=”`/usr/lib/hbase/bin/hbase classpath`:$HADOOP_CLASSPATH”
export HBASE_CONF_DIR="/etc/hbase/conf"
export PIG_USER_CLASSPATH_FIRST=true
export PIG_CLASSPATH="${HBASE_CONF_DIR}:${EXPORTHOMEDIR}/lib/dcc-etl-exporter.jar:`hbase classpath`"


#logging
umask 002
export PIG_OPTS=-Dpython.verbose=warning