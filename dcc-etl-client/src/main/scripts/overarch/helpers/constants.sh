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

# ===========================================================================
# Hosts:
DEV_PROXY="hproxy-dev"
PROD_PROXY="hproxy-dcc"

# ===========================================================================
# Email:
SMTP_SERVER_CONFIG_KEY="smtpServer"
SENDER_CONFIG_KEY="sender"
RECIPIENTS_CONFIG_KEY="recipients"

# ===========================================================================
# Run modes
NEW_RUN_MODE="new"
RE_RUN_MODE="re"

# ===========================================================================
# Components

concatenator_component="concatenator"
pruner_component="pruner"
normalizer_component="normalizer"
annotator_component="annotator"
loader_component="loader"
importer_component="importer"
summarizer_component="summarizer"
indexer_component="indexer"
stats_component="stats"
exporter_component="exporter"

special_component="lisi"

# ===========================================================================
# Dirs

fuse_mount_point="/hdfs/dcc"
overarch_cluster_output_dir="/icgc/release"

jobs_dir_name="jobs"
releases_dir_name="releases"
patches_dir_name="patches"
runs_dir_name="runs"
attempts_dir_name="attempts"

# ===========================================================================
