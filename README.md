# dcc-storage-schemas

## Introduction

This repo contains several items relate to metadata JSONs used to describe biospecimen and analysis events for the core.

First, there are JSON schema, see `analysis_flattened.json` and `biospecimen_flattened.json`.

Second, this repo contains a `generate_metadata.py` script that takes a TSV format and converts it into metadata JSON documents (and also has an option for uploading, we use this for bulk uploads to our system).

This repo also contains a merge tool, `merge_generated_metadata.py`, responsible for creating Donor centric JSON documents suitable for loading in Elasticsearch.  In the long run the idea is to use this tool to do the following:

1. query the storage system for all metadata.json
1. group the related metadata.json documents, all the docs for a given donor are grouped together
1. use the parent information in each document to understand where in the donor document the sub-documents should be merged
1. call the merge tool with sub-json documents, generate a per-donor JSON document that's suitable for loading in Elasticsearch (this includes adding various "flags" that make queries easier).
1. load in Elasticsearch, perform queries

## Install

Use python 2.7.x.

See [here](https://www.dabapps.com/blog/introduction-to-pip-and-virtualenv-python/) for information on setting
up a virtual environment for Python.

If you haven't already installed pip and virtualenv, depending on your system you may
(or may not) need to use `sudo` for these:

    sudo easy_install pip
    sudo pip install virtualenv

Now to setup:

    virtualenv env
    source env/bin/activate
    pip install jsonschema jsonmerge openpyxl sets json-spec elasticsearch

Alternatively, you may want to use Conda, see [here](http://conda.pydata.org/docs/_downloads/conda-pip-virtualenv-translator.html)
 [here](http://conda.pydata.org/docs/test-drive.html), and [here](http://kylepurdon.com/blog/using-continuum-analytics-conda-as-a-replacement-for-virtualenv-pyenv-and-more.html)
 for more information.

    conda create -n schemas-project python=2.7.11
    source activate schemas-project
    pip install jsonschema jsonmerge openpyxl sets json-spec elasticsearch

## Generate Test Metadata (and Optionally Upload Data to Storage Service)

We need to create a bunch of JSON documents for multiple donors and multiple
experimental designs and file upload types.  To do that we (Chris) developed a very simple
TSV to JSON tool and this will ultimately form the basis of our helper applications
that clients will use in the field to prepare their samples.

    python generate_metadata.py \
		--metadataSchema metadata_flattened.json \
		--outputDir output_metadata \
		--awsAccessToken `cat ucsc-storage-client/accessToken` \
		--skip-upload \
		sample_tsv/sample.tsv

Take out `--skip-upload` if you want to perform upload, see below for more details.

In case there are already existing bundle ID's that cause a collision on the S3 storage, you can specify the `--force-upload` switch to replace colliding bundle ID's with the current uploading version.

Now look in the `output_metadata` directory for per-donor directories that contain metadata files for each analysis event.

### Enabling Upload

By default the upload won't take place if the directory `ucsc-storage-client` is not present in the `dcc-storage-schema`
directory.  In order to get the client, you need to be given the tarball since it contains sensitive
information and an access key.  See our private [S3 bucket](https://s3-us-west-2.amazonaws.com/beni-dcc-storage-dev/ucsc-storage-client.tar.gz)
for the tarball.

If you have the directory setup and don't pass in `--skip-upload` the upload will take place.  Keep this in
mind if you're just testing the metadata components and don't want to create a ton of uploads.  If you upload
the fact data linked to from the `sample.tsv` the program and project will both be TEST which should make
it easy to avoid in the future. The file is based on [this](https://docs.google.com/spreadsheets/d/13fqil92C-Evi-4cy_GTnzNMmrD0ssuSCx3-cveZ4k70/edit?usp=sharing) google doc.

## Run Merge and Generate Elasticsearch Index

This tool takes multiple JSON files (see above) and merges them so we can have a donor-oriented single JSON document suitable for indexing in Elasticsearch.  It takes a list of directories that contain *.json files.  In this case, I'm
using the output from the generate_metadata.py script.

    python merge_generated_metadata.py `for i in output_metadata/*; do echo -n "$i "; done`

This produces a `merge.jsonl` file which is actually a JSONL file, e.g. each line is a JSON document.
Now to view the output for the first line use the following:

    cat merge.jsonl | head -1 | json_pp | less -S

You can also examine this in Chrome using the JSONView extension.  Make sure you select
the option to allow viewing of local JSON files before you attempt to load this
file in Chrome.  The commands below will display the second JSON document. On a Mac:

    cat merge.jsonl | head -2 | tail -1 | json_pp > temp.json
    open -a Google\ Chrome temp.json

## Load and Query Elasticsearch

In the query_on_merge folder, you will find a queryable document, compact_single.json and a sample query, jquery1.
Start by running Elasticsearch, then to add the compact_single.json to your node by

    curl -XPUT http://localhost:9200/analysis_index/_bulk?pretty --data-binary @elasticsearch.jsonl

Then check to see if index has been created. (Should have five documents).

    curl 'localhost:9200/_cat/indices?v'

Query everything.

    curl -XGET http://localhost:9200/analysis_index/_search?pretty

And query.

    curl -XPOST http://localhost:9200/analysis_index/_search?pretty -d @query_on_merge/jquery1

Since merge.py now adds flags, you can find a queryable document, mergeflag.json and sample queries, jqueryflag. Add this document in the same fashion to a new index. Then query with:

    curl -XPOST http://localhost:9200/analysis_index/_search?pretty -d @query_on_merge/jqueryflag

However, the problem with this method is that only the first query is performed.

esquery.py can perform all of the queries (elasticsearch needs to be installed. pip install elasticsearch). Run using:

    python esquery.py

If running esquery.py multiple times, remove the index with:

    curl -XDELETE http://localhost:9200/analysis_index

## Demo

Goal: create sample single donor documents and perform queries on them.

1. Install the needed packages as described above.
1. Generate metadata for multiple donors using `generate_metadata.py`, see command above
1. Create single donor documents using `merge_generated_metadata.py`, see command above
1. Load into ES index, see `curl -XPUT` command above
1. Run the queries using `esquery.py`, see command above
1. Optionally, deleted the index using the `curl -XDELETE` command above

The query script, `esquery.py`, produces output whose first line prints the number of documents searched upon.
The next few lines are center, program and project.
Following those lines, are the queries, which give information on:
* specifications of the query
* number of documents that fit the query
* number of documents that fit this query for a particular program
* project name

## Data Types

We support the following types.  First and foremost, the types below are just intended
to be an overview. We need to standardize on actual acceptable terms. To do this
we use the Codelists (controlled vocabularies) from the ICGC.  See http://docs.icgc.org/dictionary/viewer/#?viewMode=codelist

In the future we will validate metadata JSON against these codelists.

### Sample Types:

* dna normal
* dna tumor
* rna tumor
* rna normal (rare)

And there are others as well but these are the major ones we'll encounter for now.

The actual values should come from the ICGC Codelist above.  Specifically the
`specimen.0.specimen_type.v3` codelist.

### Experimental Design Types

* WXS
* WGS
* Gene Panel
* RNAseq

The actual values should come from the ICGC Codelist above.  Specifically the
`GLOBAL.0.sequencing_strategy.v1` codelist.

### File Types/Formats

* sequence/fastq
* sequence/unaligned BAM
* alignment/BAM & BAI pair
* expression/RSEM(?)
* variants/VCF

These will all come from the [EDAM Ontology](http://edamontology.org).  They have
a mechanism to add terms as needed.

### Analysis Types

* germline_variant_calling -> normal specimen level
* rna_quantification (and various other RNASeq-based analysis) -> tumor specimen level
* somatic_variant_calling -> tumor specimen level (or donor if called simultaneously for multiple tumors)
* immuno_target_pipelines -> tumor specimen level

Unfortunately, the CVs from ICGC don't cover the above, see [here](http://docs.icgc.org/dictionary/viewer/#?viewMode=table).
Look for items like `variation_calling_algorithm` and you'll see they are actually just
TEXT with a regular expression to validate them.

Take home, I think we use our own CV for these terms and expand it over time here.

I think we also need to support tagging with multiple EDAM terms as well which can,
together, describe what I'm trying to capture above.  For example:

germline_variant_calling could be:

* [Variant calling](http://edamontology.org/operation_3227): http://edamontology.org/operation_3227

Which isn't very specific and the description sounds closer to somatic calling.

So this argues that we should actually just come up with our own specific terms
used for the institute since we aren't attempting to capture the whole world's
possible use cases here.

Over time I think this will expand.  Each are targeted at a distinct biospecimen "level".
This will need to be incorporated into changes to the index builder.

## TODO

* need to add upload to Chris' script
* need to download all the metadata from the storage service
* use the above two to show end-to-end process, develop very simple cgi script to display table
* each workflow JSON needs a timestamp
* command line tool would merge the docs, taking the "level" at which each document will be merged in at
    * donor, sample, specimen

In the future, look at adding options here for specifying where files should be merged:

    python merge.py --sample alignment:alignment1.json --sample alignment:alignment2.json --donor somatic_variant_calling:somatic.json
