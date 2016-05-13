# dcc-storage-schemas

## Introduction

Repo contains sample schemas.

This repo also contains a merge tool.  The idea is to:

0. query the storage system for all metadata.json
0. group the related metadata.json documents, all the docs for a given donor are grouped together
0. use the parent information in each document to understand where in the donor document the sub-documents should be merged
0. call the merge tool with sub-json documents, generate a per-donor document
0. load in Elasticsearch

## Install

See https://www.dabapps.com/blog/introduction-to-pip-and-virtualenv-python/

    virtualenv env
    source env/bin/activate
    pip install jsonschema
    pip install jsonmerge

## Run Merge

This tool takes multiple JSON and merges them so we can have a donor-oriented single JSON document suitable for indexing in Elasticsearch.

    python merge.py

In the future, look at adding options here for specifying where files should be merged:

    python merge.py --sample alignment:alignment1.json --sample alignment:alignment2.json --donor somatic_variant_calling:somatic.json

## Ideas:

* could use merge tool
* command line tool would merge the docs, taking the "level" at which each document will be merged in at
    * donor, sample, specimen
