#!/bin/bash -e
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


export run_type="prod"
export config_file="$HOME/dcc-etl/conf/etl_prod.yaml"

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
