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

import os
from org.apache.pig.scripting import *
from org.apache.hadoop.hbase import HBaseConfiguration

########USER-DEFINED SETTINGS###########
release = "default"
data_source = '/icgc/etl/default'  # this needs to be updated for every new release
upload = None

########CONTROL SETTINGS (INTERNAL) ###########
root = "/tmp/download"
root_out_static = root + "/static"
tmp = root + "/tmp"
tmp_dynamic_root = tmp + '/dynamic'
tmp_hfile_root = tmp + '/hfile'
tmp_static_root = tmp + '/static'
tmp_bucket_root = tmp + '/bucket'
tmp_index = tmp + '/fullindex'
out_dynamic = root + '/dynamic/'
root_validation = root + '/validation'
root_backup = "/nfs/backups/dcc-download-images"

default_exporter_src = '.'

part = '/*/part-*'
# loader = 'com.twitter.elephantbird.pig.load.JsonLoader'
loader = 'com.twitter.elephantbird.pig.load.LzoJsonLoader'

########HADOOP MAPREDUCE ###########
Pig.set("io.sort.mb", "500")
Pig.set("io.sort.factor", "50")
# Pig.set("pig.splitCombination","false")
Pig.set("mapred.map.tasks.speculative.execution", "false")
Pig.set("mapred.reduce.tasks.speculative.execution", "false")
Pig.set("mapreduce.task.classpath.user.precedence", "true")
Pig.set("dfs.client.read.shortcircuit", "true")
Pig.set("io.file.buffer.size", "131072")


######## COMPRESSION ###########
Pig.set("io.compression.codecs","org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.SnappyCodec")
# Map Output #
Pig.set("mapred.compress.map.output", "true")
# Pig.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.GzipCodec")
Pig.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec")

# MapRed intermediate output (Pig control) #
Pig.set("pig.tmpfilecompression", "true")
# Pig.set("pig.tmpfilecompression.codec","gz")
Pig.set("pig.tmpfilecompression.codec", "lzo")

# Actual output (Client) #
#Pig.set("output.compression.enabled", "true")
#Pig.set("output.compression.codec", "com.hadoop.compression.lzo.LzopCodec")
# Pig.set("output.compression.codec","org.apache.hadoop.io.compress.GzipCodec")
# Pig.set("zlib.compress.level", "BEST_SPEED")

######## HBASE ###########
conf = HBaseConfiguration.create()

Pig.set("hbase.zookeeper.quorum", conf.get("hbase.zookeeper.quorum"))
Pig.set("hbase.zookeeper.property.clientPort", conf.get("hbase.zookeeper.property.clientPort"))
Pig.set("hbase.client.write.buffer","5242880")

######## GLOBAL ##########
# mapping between the loader and the export data types
data = {
        'ssm_controlled': 'ssm',
        'ssm_open': 'ssm',
        'clinical': 'donor',
        'clinicalsample' : 'donor',

        'donor': 'donor',
        'specimen': 'donor',
        'donor_therapy': 'donor',
        'donor_exposure': 'donor',
        'donor_family': 'donor',
        'sample': 'donor',

        'cnsm' : 'cnsm',
        'jcn' : 'jcn',
        'meth_seq' : 'meth_seq',
        'meth_array' : 'meth_array',
        'mirna_seq' : 'mirna_seq',
        'stsm' : 'stsm',
        'pexp' : 'pexp',
        'exp_seq' : 'exp_seq',
        'exp_array' : 'exp_array',
        'sgv_controlled' : 'sgv'
       }
       
summary = {
        'ssm_controlled': False,
        'ssm_open': False,
        'clinical': False,
        'clinicalsample' : False,

        'donor': True,
        'specimen': True,
        'donor_therapy': True,
        'donor_exposure': True,
        'donor_family': True,
        'sample': True,

        'cnsm' : False,
        'jcn' : False,
        'meth_seq' : False,
        'meth_array' : False,
        'mirna_seq' : False,
        'stsm' : False,
        'pexp' : False,
        'exp_seq' : False,
        'exp_array' : False,
        'sgv_controlled' : False
        }
        
# mapping from short name to long name for each data type
longname = {
        'ssm_controlled' : 'simple_somatic_mutation.controlled',
        'ssm_open' : 'simple_somatic_mutation.open',
        'clinical' : 'clinical',
        'donor': 'donor',
        'specimen': 'specimen',
        'donor_therapy': 'donor_therapy',
        'donor_exposure': 'donor_exposure',
        'donor_family': 'donor_family',
        'sample': 'sample',
        'clinicalsample' : 'clinicalsample',
        'cnsm' : 'copy_number_somatic_mutation',
        'jcn' : 'splice_variant',
        'meth_seq' : 'meth_seq',
        'meth_array' : 'meth_array',
        'mirna_seq' : 'mirna_seq',
        'stsm' : 'structural_somatic_mutation',
        'pexp' : 'protein_expression',
        'exp_seq' : 'exp_seq',
        'exp_array' : 'exp_array',
        'sgv_controlled' : 'simple_germline_variation.controlled'
       }

######## Utility ##################
def touch(fname, times=None):
    fhandle = open(fname, 'a')
    try:
        os.utime(fname, times)
    finally:
        fhandle.close()