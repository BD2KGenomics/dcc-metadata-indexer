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
experimental designs and file upload types.  To do that we (Chris) developed a very simple
TSV to JSON tool and this will ultimately form the basis of our helper applications
that clients will use in the field to prepare their samples.

    python generate_metadata.py -v \
		--biospecimenSchema biospecimen_flattened.json \
		--analysisSchema analysis_flattened.json \
		sample_tsv/sample.tsv

## Run Merge

This tool takes multiple JSON and merges them so we can have a donor-oriented single JSON document suitable for indexing in Elasticsearch.

    python merge.py

Now to view the output:

    cat merge.json | json_pp | less -S

You can also examine this in Chrome using the JSONView extension.  Make sure you select
the option to allow viewing of local JSON files before you attempt to load this
file in Chrome. On a Mac:

    open -a Google\ Chrome merge.json

## Query using Elasticsearch

In the query_on_merge folder, you will find a queryable document, compact_single.json and a sample query, jquery1.
Start by running Elasticsearch, then to add the compact_single.json to your node by
    
    curl -XPUT http://localhost:9200/name_of_index/_bulk?pretty --data-binary @compact_single.json

Then check to see if index has been created. (Should have five documents).

    curl 'localhost:9200/_cat/indices?v'

And query.

    curl -XPOST http://localhost:9200/name_of_index/_search?pretty -d @jquery1

Since merge.py now adds flags, you can find a queryable document, mergeflag.json and sample queries, jqueryflag. Add this document in the same fashion to a new index. Then query with:

    curl -XPOST http://localhost:9200/name_of_index/_search?pretty -d @jqueryflag

However, the problem with this method is that only the first query is performed.

esquery.py can perform all of the queries (elasticsearch needs to be installed. pip install elasticsearch). Run using: 

    python3.5 esquery.py

If running esquery.py multiple times, remove the index with:

    curl -XDELETE http://localhost:9200/name_of_index

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

## Ideas

* each workflow JSON needs a timestamp
* command line tool would merge the docs, taking the "level" at which each document will be merged in at
    * donor, sample, specimen

In the future, look at adding options here for specifying where files should be merged:

    python merge.py --sample alignment:alignment1.json --sample alignment:alignment2.json --donor somatic_variant_calling:somatic.json
