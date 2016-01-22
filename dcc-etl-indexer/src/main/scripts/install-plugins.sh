#!/bin/sh
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
#   This script is used to install the commonly used elasticsearch plugins. Most of these are
#   so called "site" plugins. They are very useful for admin, monitoring, index development and
#   querying.
#
# See:
#   http://www.elasticsearch.org/guide/reference/modules/plugins/
#
# Typical plugin script locations:
#   /usr/local/
#   /usr/share/elasticsearch/
#
# SCM:
#  https://github.com/icgc-dcc/dcc/blob/develop/dcc-etl/dcc-etl-indexer/src/main/scripts/install-plugins.sh
#

# Change to ES home directory (see above)
cd /usr/local/

# See https://github.com/mobz/elasticsearch-head
bin/plugin -remove head
bin/plugin -install mobz/elasticsearch-head

# See https://github.com/lukas-vlcek/bigdesk
bin/plugin -remove bigdesk
bin/plugin -install lukas-vlcek/bigdesk

# See https://github.com/karmi/elasticsearch-paramedic
bin/plugin -remove paramedic
bin/plugin -install karmi/elasticsearch-paramedic

# See https://github.com/polyfractal/elasticsearch-segmentspy
bin/plugin -remove segmentspy
bin/plugin -install polyfractal/elasticsearch-segmentspy

# See https://github.com/polyfractal/elasticsearch-inquisitor
bin/plugin -remove inquisitor
bin/plugin -install polyfractal/elasticsearch-inquisitor

# See https://github.com/royrusso/elasticsearch-HQ
bin/plugin -remove HQ
bin/plugin -install royrusso/elasticsearch-HQ

# See https://github.com/jprante/elasticsearch-knapsack
bin/plugin -remove knapsack
bin/plugin -url http://dl.bintray.com/jprante/elasticsearch-plugins/org/xbib/elasticsearch/plugin/elasticsearch-knapsack/2.0.0/elasticsearch-knapsack-2.0.0.zip?direct -install knapsack

