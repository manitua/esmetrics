ESmetrics
=========
[![Build Status](https://travis-ci.org/manitua/esmetrics.svg?branch=master)](https://travis-ci.org/manitua/esmetrics)
[![GoDoc](https://godoc.org/github.com/manitua/esmetrics?status.svg)](https://godoc.org/github.com/manitua/esmetrics)


Grab Elasticsearch cluster status and performance data and store it in Graphite for use in Graphana dashboard.

Usage
-----
    Usage of ./esmetrics:
      -eh string
        	IP, FQDN or hostname of your ElasticSearch host
      -ep int
        	ElasticSearch hosts port (default 9200)
      -gd string
        	Graphite database name (default "elasticsearch.cluster")
      -gh string
        	IP, FQDN or hostname of your Carbon host
      -gp int
        	Carbon hosts port (default 2003)
      -poll duration
        	Metrics poll interval (default 20s)
