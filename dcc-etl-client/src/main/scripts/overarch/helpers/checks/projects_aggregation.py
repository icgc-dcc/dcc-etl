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

# Checks that donor counts per data types are correct
# Usage: checks/projects_aggregation [index dump] [project paths]
# ===========================================================================
import sys,json,re,glob,gzip,bz2
# ===========================================================================

DEFAULT_PATH="/hdfs/dcc/icgc/submission/ICGC16"
COUNTS = [
		"_total_donor_count" ,
		"_total_specimen_count" ,
		"_total_sample_count" ,
		"_ssm_tested_donor_count" ,
		"_sgv_tested_donor_count" ,
		"_cnsm_tested_donor_count" ,
		"_stsm_tested_donor_count" ,
		"_meth_array_tested_donor_count" ,
		"_meth_seq_tested_donor_count" ,
		"_mirna_seq_tested_donor_count" ,
		"_exp_array_tested_donor_count" ,
		"_exp_seq_tested_donor_count" ,
		"_pexp_tested_donor_count" ,
		"_jcn_tested_donor_count"
	]

# ===========================================================================

index_dump_file = sys.argv[1] # curl http://***REMOVED***:9200/dcc-release-load-prod-08d-49-icgc16-3/project/_search?pretty=1 -d '{size:1000}'
projects_json_file = sys.argv[2] # in git: dcc-etl/dcc-etl-client/src/main/scripts/overarch/conf/icgc16.json

# ===========================================================================

def get_data_type(count):
	return re.sub("_count$", "", re.sub("^total_", "", re.sub("^_", "", count.replace("_tested_donor_count", ""))))

# ---------------------------------------------------------------------------

def is_clinical(data_type):
	return data_type == "donor" or data_type == "specimen" or data_type == "sample"

# ---------------------------------------------------------------------------

def is_plain_file(input):
   return re.search("\.txt$", input)
def is_gzip_file(input):
   return re.search("\.txt\.gz$", input)
def is_bzip2_file(input):
   return re.search("\.txt\.bz2$", input)
def open_file(input_file):
	if is_plain_file(input_file):
		fd = open(input_file)
	elif is_gzip_file(input_file):
		fd = gzip.open(input_file)
	elif is_bzip2_file(input_file):
		fd = bz2.BZ2File(input_file)
	else:
		assert False
	return fd

# ---------------------------------------------------------------------------

def get_content(path):
	files = glob.glob(path)
	data = []
	for file in files:
		f = open_file(file)
		for line in f:
			data.append(line.strip('\n').strip('\r').split('\t'))
		f.close()
	return data

# ---------------------------------------------------------------------------

def get_clinical_map(specimen_path, sample_path):
	specimen_data = get_content(specimen_path)
	sample_data = get_content(sample_path)

	specimen2donor = {}
	for specimen_row in specimen_data:
		donor_id = specimen_row[0];
		specimen_id = specimen_row[1];
		specimen2donor[specimen_id] = donor_id

	sample2specimen = {}
	for sample_row in sample_data:
		sample_id = sample_row[0];
		specimen_id = sample_row[1];
		sample2specimen[sample_id] = specimen_id

	sample2donor = {}
	for sample_id in sample2specimen:
		sample2donor[sample_id] = specimen2donor[sample2specimen[sample_id]]

	return sample2donor

# ---------------------------------------------------------------------------

def process_data_type(data_type, file_type, path, expected_count, clinical_map):

	# List all matching files
	files = glob.glob(path)

	# If no match, make sure we expect none
	if not files:
		assert expected_count == 0

	else:

		# Perform simple line count for clinical files
		if is_clinical(file_type):
			actual_count = 0
			for file in files:
				f = open_file(file)
				for line in f:
					actual_count += 1
				f.close()
				actual_count -= 1 # header

		# For observations, perform a unique count of donors
		else:
			donor_ids = []
			for file in files:
				f = open_file(file)
				for line in f:
					sample_id = line.strip('\n').strip('\r').split('\t')[1]
					donor_id = clinical_map[sample_id]
					donor_ids.append(donor_id)
				f.close()
			actual_count = len(set(donor_ids))-1 # -1 for headers
		assert actual_count == expected_count, "%s, %s, %s" % (expected_count, actual_count, path)
		print "\t%s: OK" % data_type

# ===========================================================================

# Read up index dump to get counts
with open(index_dump_file) as f:
	docs = json.load(f)
hits = docs["hits"]["hits"]

# ---------------------------------------------------------------------------

# Prepare displayable table of counts per project/type, print and persist it
display_string = "project_key"
for count in COUNTS:
	display_string += '\t' + get_data_type(count)
display_string += '\n'
for hit in hits:
	project_key = hit["_id"]
	summary = hit["_source"]["_summary"]
	display_string += project_key
	for count in COUNTS:
		display_string += '\t' + str(summary[count])
	display_string += '\n'
with open("/tmp/projects.tsv", 'w') as f:
	f.write(display_string)
print display_string

# ---------------------------------------------------------------------------

# Gather same data as map
data = {}
for hit in hits:
	project_key = hit["_id"]
	summary = hit["_source"]["_summary"]
	data[project_key] = {}
	for count in COUNTS:
		data[project_key][get_data_type(count)] = summary[count]

# ---------------------------------------------------------------------------

# Read up project file paths file
with open(projects_json_file) as f:
	projects_json = json.load(f)

# ---------------------------------------------------------------------------

# Initialize default path where needed
for project_key in projects_json:
	project_paths = projects_json[project_key]
	if "parent_dir" not in project_paths:
		project_paths["parent_dir"] = DEFAULT_PATH

# ---------------------------------------------------------------------------

# for each project and each type, make sure count from index matches counts from original files
for project_key in data:
	print project_key
	project = data[project_key]
	project_paths = projects_json[project_key]

	# Build correct path per type
	paths = {}
	file2data = {}
	for data_type in project:
		if is_clinical(data_type):
			file_type = data_type
		else:
			file_type = data_type + "_m"
		file_type_key = file_type + "_file"
		if file_type_key in project_paths:
			path = project_paths[file_type_key]
		else:
			path = project_paths["parent_dir"] + '/' + project_key + '/' + file_type + "*"
		paths[file_type] = path
		file2data[file_type]=data_type

	# Get sample to donor ID map
	clinical_map = get_clinical_map(paths["specimen"], paths["sample"])

	# For each type, perform check
	for file_type in paths:
		data_type = file2data[file_type]
		count = project[data_type]
		path = paths[file_type]
		process_data_type(data_type, file_type, path, count, clinical_map)

# ===========================================================================

