# dcc-storage-schemas

## Introduction

Repo contains sample schemas.

This repo also contains a merge tool.  The idea is to:

1. query the storage system for all metadata.json
1. group the related metadata.json documents, all the docs for a given donor are grouped together
1. use the parent information in each document to understand where in the donor document the sub-documents should be merged
1. call the merge tool with sub-json documents, generate a per-donor document
1. load in Elasticsearch

## Install

See https://www.dabapps.com/blog/introduction-to-pip-and-virtualenv-python/

If you haven't already installed pip and virtualenv, depending on your system you may
(or may not) need to use `sudo` for these:

    sudo easy_install pip
    sudo pip install virtualenv

Now to setup:

    virtualenv env
    source env/bin/activate
    pip install jsonschema
    pip install jsonmerge

Alternatively, you may want to use Conda, see http://conda.pydata.org/docs/_downloads/conda-pip-virtualenv-translator.html and http://kylepurdon.com/blog/using-continuum-analytics-conda-as-a-replacement-for-virtualenv-pyenv-and-more.html.

    conda create -n schemas-project python
    source activate schemas-project
    pip install jsonschema
    pip install jsonmerge

## Generate Test Data

We need to create a bunch of JSON documents for multiple donors and multiple
experimental designs and file upload types.  To do that I developed a very simple
TSV to JSON tool and this will ultimately form the basis of our helper applications
that clients will use in the field to prepare their samples.

TODO

## Run Merge

This tool takes multiple JSON and merges them so we can have a donor-oriented single JSON document suitable for indexing in Elasticsearch.

    python merge.py

Now to view the output:

    cat merge.json | json_pp | less -S

You can also examine this in Chrome using the JSONView extension.  Make sure you select
the option to allow viewing of local JSON files before you attempt to load this
file in Chrome. On a Mac:

    open -a Google\ Chrome merge.json

## Data Types

We support the following types:

### Sample Types:

* dna normal
* dna tumor
* rna tumor
* rna normal (rare)

And there are others as well but these are the major ones we'll encounter for now.

### Experimental Design Types

* WXS
* WGS
* Gene Panel
* RNAseq

### File Types/Formats

* sequence/fastq
* sequence/unaligned BAM
* alignment/BAM & BAI pair
* expression/RSEM(?)
* variants/VCF

### Analysis Types

* germline_variant_calling -> normal specimen level
* rna_quantification (and various other RNASeq-based analysis) -> tumor specimen level
* somatic_variant_calling -> tumor specimen level (or donor if called simultaneously for multiple tumors)
* immuno_target_pipelines -> tumor specimen level

Over time I think this will expand.  Each are targeted at a distinct biospecimen "level".

## Ideas

* each workflow JSON needs a timestamp
* command line tool would merge the docs, taking the "level" at which each document will be merged in at
    * donor, sample, specimen

In the future, look at adding options here for specifying where files should be merged:

    python merge.py --sample alignment:alignment1.json --sample alignment:alignment2.json --donor somatic_variant_calling:somatic.json
