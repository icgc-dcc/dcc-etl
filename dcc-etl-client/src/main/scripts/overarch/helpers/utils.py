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

import sys, os, json

def check_file_exists(input_file):
	assert os.path.isfile(input_file), "Couldn't find file: '%s'" % input_file
	return input_file

def read_json_file(json_file):
	with open(check_file_exists(json_file)) as f:
		json_doc = json.load(f)
	return json_doc

def get_project_keys(projects_json_file):
	json_doc = read_json_file(projects_json_file)
	keys = list(json_doc.keys())
	keys.sort()
	return ' '.join([str(key) for key in keys])

# Returns the password matching the given prefixed key in the given password file (key-value pairs)
def get_password(password_file, prefix, key):
	assert os.path.isfile(password_file), "'%s' does not exist" % password_file
	key = "%s.%s" % (prefix, key)
	with open(password_file, 'r') as f:
		for line in f.readlines():
			if line.startswith(key + '='):
				split = line.strip().split('=')
				assert len(split) == 2, "Unexpected format for line in password file: '%s'" % line
				value = split[1]
				assert value, "Unexpected value for key: '%s'" % key
				return value
	assert False, "Could not find any match for: '%s'" % key

