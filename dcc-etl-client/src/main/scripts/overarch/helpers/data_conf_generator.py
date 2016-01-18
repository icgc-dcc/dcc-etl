#!/usr/bin/python
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

# Helps generating the JSON file that describes where to find data to the overarch ETL script, mostly meant to be a starting point
# One can get the list of signed off projects with something like: mongo <mongo_url>/icgc -u***REMOVED*** -p$REPLY --quiet --eval "db.Release.find({name:'ICGC16'}, {name:1, 'submissions.projectKey': 1, 'submissions.state': 1}).forEach(printjson)" | grep -B1 '"SIGNED_OFF"' | awk -v RS=ignoreme '{gsub(/,\n/,"\t")}1' | awk -F'"' '{print $4}' | awk '!/^ #/ && !/^[ \t]$/ && !/^$/' | sort
# ===========================================================================
import json,re,os

previous_release_dir="/hdfs/dcc/icgc/submission/ICGC15"
migration_dir="/nfs/dcc_secure/dcc/etl/icgc16/migration"

signed_off_projects_included=["ALL-US", "BLCA-CN", "BLCA-US", "BRCA-US", "CESC-US", "CLLE-ES", "COAD-US", "EOPC-DE", "ESCA-CN", "GACA-CN", "GBM-US", "HNSC-US", "KIRC-US", "KIRP-US", "LAML-US", "LGG-US", "LICA-FR", "LIHC-US", "LIRI-JP", "LUAD-US", "LUSC-US", "MALY-DE", "NBL-US", "ORCA-IN", "OV-AU", "OV-US", "PAAD-US", "PACA-AU", "PACA-CA", "PAEN-AU", "PBCA-DE", "PRAD-UK", "PRAD-US", "READ-US", "RECA-CN", "SKCM-US", "STAD-US", "THCA-US", "UCEC-US"]
signed_off_temporarily_excluded=["LAML-KR", "LUSC-KR"]
signed_off_permanently_excluded=["AML-US", "WT-US"] # no experimental data, ...

postprocessed_project_keys = ["BRCA-UK", "LAML-KR", "LUSC-KR"]

migration_projects=["BOCA-UK", "CMDI-UK", "ESAD-UK", "LINC-JP", "PRAD-CA", "THCA-SA"]
migration_special_project_keys= ["RECA-EU"]

#migration_temporarily_excluded=[]

doc = json.loads("{}");
for project_key in signed_off_projects_included:
	project = {}
	doc[project_key] = project

for project_key in migration_projects:
	project = {}
	project["parent_dir"] = previous_release_dir
	project["sample_file"] = "%s/sample/%s/sample.txt" % (migration_dir, project_key)
	doc[project_key] = project

for project_key in postprocessed_project_keys:
	doc[project_key] = { "parent_dir": "%s/all" % (migration_dir) }

migration_special_project_key="RECA-EU"
doc[migration_special_project_key] = { \
		"parent_dir": "%s/special" % (migration_dir), \
		"sample_file": "%s/sample/%s/sample.txt" % (migration_dir, migration_special_project_key), \
		"ssm_p_file": "%s/ssm_p/%s/ssm_p.txt" % (migration_dir, migration_special_project_key), \
	} # expression data is ignored as we can't migrate it

print json.dumps(doc, sort_keys=True, indent=4)

print
print len(signed_off_projects_included)
print len(postprocessed_project_keys)
print len(migration_projects)
print len(migration_special_project_keys)
print
print len(signed_off_projects_included)+len(postprocessed_project_keys)+len(migration_projects)+len(migration_special_project_keys)
