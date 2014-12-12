#!/usr/bin/python
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

