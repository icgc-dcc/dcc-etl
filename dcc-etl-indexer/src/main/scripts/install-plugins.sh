#!/bin/sh
#
# Copyright 2013(c) The Ontario Institute for Cancer Research. All rights reserved.
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

