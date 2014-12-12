# Prints stats on a specified ElasticSearch index
# usage: ./elt-stats-es dcc-release-release5
#

curl 'http://***REMOVED***:9200/$1/_stats?types=donor-centric&pretty=true'
