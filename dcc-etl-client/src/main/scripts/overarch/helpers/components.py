#!/usr/bin/python
# Returns a canonicalized list of components to run based on the shorthand provided
#  (e.g. "normalizer-importer" -> "normalizer,annotator,mongoinit,hdfsput,loader,importer" or "importer-" -> "importer,summarizer,indexer,stats,exporter,vcfcopy")
# examples:
#    ./components.py concatenator,normalizer,annotator,lisi,exporter concatenator # concatenator
#    ./components.py concatenator,normalizer,annotator,lisi,exporter concatenator-normalizer # concatenator,normalizer
#    ./components.py concatenator,normalizer,annotator,lisi,exporter concatenator-normalizer,loader # concatenator,normalizer,loader
#    ./components.py concatenator,normalizer,annotator,lisi,exporter -normalizer,loader # concatenator,normalizer,loader
#    ./components.py concatenator,normalizer,annotator,lisi,exporter hdfsput- # hdfsput,loader,importer,summarizer,indexer,stats,exporter,vcfcopy
#    ./components.py concatenator,normalizer,annotator,lisi,exporter - # concatenator,normalizer,annotator,lisi,exporter
#    ./components.py concatenator,normalizer,annotator,lisi,exporter all # concatenator,normalizer,annotator,lisi,exporter

import sys

ordered_components_csv = sys.argv[1] # ordered
value = sys.argv[2]

print "ordered_components: '%s'" % ordered_components_csv
print "value: '%s'" % value

# Ensure no retitions
ordered_components = ordered_components_csv.split(',')
assert len(ordered_components) >= 1
assert len(ordered_components) == len(set(ordered_components))

if value=="all": # same as '-'
	print "ok: " + ' '.join(split)
	exit()

components_to_run = []
commas = value.split(',')
item_count = 0
for item in commas:
	if '-' in item:
		dashes = item.split('-')
		assert len(dashes)==2, dashes
		from_component = dashes[0]
		to_component = dashes[1]
		if not from_component:
			assert item_count == 0 # is first item
			from_component = ordered_components[0] # assign first 
		if not to_component:
			assert item_count == len(commas)-1 # is last item	
			to_component = ordered_components[len(ordered_components)-1] # assign last
		assert from_component in ordered_components
		assert to_component in ordered_components
		print "from component: %s" % from_component
		print "to component: %s" % to_component

		on = False
		for item in ordered_components:
			if not on and item == from_component:
				on = True
			if on:
				components_to_run.append(item)
			if on and item == to_component:
				on = False
	else:
		assert item in ordered_components
		components_to_run.append(item)
	item_count = item_count + 1

previous = None
for component_to_run in components_to_run:
	i = ordered_components.index(component_to_run)
	assert previous is None or i>previous
	previous = i

print "ok: " + ' '.join(components_to_run)
exit()

