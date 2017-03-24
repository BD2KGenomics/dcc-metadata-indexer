# dcc-metadata-indexer

## Introduction

This repo contains several items relate to the metadata indexing process we use to describe biospecimen and analysis events for the core.

First, there are JSON schema, see `analysis_schema.json` and `metadata_schema.json`.

Second, this repo contains a metadata index building tool, `merge_gen_meta.py`, responsible for creating Donor centric JSON documents suitable for loading in Elasticsearch from the content of a [Redwood](https://github.com/BD2KGenomics/dcc-redwood-storage) storage system.  The idea is to use this tool to do the following:

1. query the storage system for all metadata.json
1. group the related metadata.json documents, all the docs for a given donor are grouped together
1. use the parent information in each document to understand where in the donor document the sub-documents should be merged
1. call the merge tool with sub-json documents, generate a per-donor JSON document that's suitable for loading in Elasticsearch (this includes adding various "flags" that make queries easier).
1. load in Elasticsearch, perform queries for the web [Dashboard](https://github.com/BD2KGenomics/dcc-dashboard) or for the [Action Service](https://github.com/BD2KGenomics/dcc-action-service)

## Install

### Ubuntu 14.04

You need to make sure you have system level dependencies installed in the appropriate way for your OS.  For Ubuntu 14.04 you do:

    sudo apt-get install python-dev libxml2-dev libxslt-dev lib32z1-dev

### Python

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
    pip install jsonschema jsonmerge openpyxl sets json-spec elasticsearch semver luigi python-dateutil

Alternatively, you may want to use Conda, see [here](http://conda.pydata.org/docs/_downloads/conda-pip-virtualenv-translator.html),
 [here](http://conda.pydata.org/docs/test-drive.html), and [here](http://kylepurdon.com/blog/using-continuum-analytics-conda-as-a-replacement-for-virtualenv-pyenv-and-more.html)
 for more information.

    conda create -n schemas-project python=2.7.11
    source activate schemas-project
    pip install jsonschema jsonmerge openpyxl sets json-spec elasticsearch semver luigi python-dateutil

### Redwood Client

In order to get the client, you need to be given an access key and download our client tarball.  See our public [S3 bucket](https://s3-us-west-2.amazonaws.com/beni-dcc-storage-dev/20161216_ucsc-storage-client.tar.gz)
for the tarball.

## Run Merge and Generate Elasticsearch Index

This tool takes `metadata.json` files from the Redwood storage service (see above) and merges them so we can have a donor-oriented single JSON document suitable for indexing in Elasticsearch.  This command will read and download the json files from the storage system endpoint. In addition to creating a `validated.jsonl` file it will also create a `endpoint_metadata/` directory that contains all of the json files that were downloaded.

    python metadata_indexer.py --only-program TEST --only-project TEST --storage-access-token `cat ucsc-storage-client/accessToken.2`  --client-path ucsc-storage-client/ --metadata-schema metadata_schema.json --server-host storage2.ucsc-cgl.org

The command below will not download json files, instead the user will provide a directory that contains json files.

    python metadata_indexer.py --only-program TEST --only-project TEST --storage-access-token `cat ucsc-storage-client/accessToken`  --client-path ucsc-storage-client/ --metadata-schema metadata_schema.json --server-host storage2.ucsc-cgl.org --test-directory output_metadata_7_20/

This produces a `validated.jsonl` and a `invalid.jsonl` file which is actually a JSONL file, e.g. each line is a JSON document.  It also produces an `elasticsearch.jsonl` which has the same content but is suitable for loading in Elasticsearch.

Now to view the output for the first line use the following:

    cat validated.jsonl | head -1 | json_pp | less -S

You can also examine this in Chrome using the JSONView extension.  Make sure you select
the option to allow viewing of local JSON files before you attempt to load this
file in Chrome.  The commands below will display the second JSON document. On a Mac:

    cat validated.jsonl | head -2 | tail -1 | json_pp > temp.json
    open -a Google\ Chrome temp.json

## Load and Query Elasticsearch

In the query_on_merge folder, you will find a queryable document, compact_single.json and a sample query, jquery1.
Start by running Elasticsearch, then to add the compact_single.json to your node by

    curl -XPUT http://localhost:9200/analysis_index/_bulk?pretty --data-binary @elasticsearch.jsonl

Then check to see if index has been created. (Should have five documents).

`curl 'localhost:9200/_cat/indices?v'`

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
1. Create single donor documents using `merge_gen_meta.py`, see command above
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

### DateTime

We should standardize on [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) for string representations.

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

## Docker version

The repo includes a Dockerfile that can be used to construct a docker image. To build the image, you can run the following within the directory:

```
docker build -t <image_name> .
```

The image is based on Alpine, so as to minimize the image's space. The image has an entrypoint script that runs the metadata_indexer.py script. You can still use the same flags used from the metadata-indexer. In addition, you can use `--help | -h` to get more information about the supported flags.

In order to run the image, you have to mount 2 folders (with an additional one being optional):

* For the output folder with the metadata
* For the .jsonl files (used to load elasticsearch)
* For reading and redacting files (optional)

To sample run of the image would look something like this:

```
docker run -e USER_GROUP=<USER:GROUP> -e ES_SERVICE=localhost -e DATABASE_URL=<DB_URL> -v <FOLDER_WITH_JSONLs>:/app/dcc-metadata-indexer/es-jsonls \
-v <OUTPUT_METADATA_FOLDER>:/app/dcc-metadata-indexer/endpoint_metadata \
-v <REDACTED_FOLDER>:/app/dcc-metadata-indexer/redacted -p 9200:9200 \
<DOCKER_IMAGE_NAME> --storage-access-token <YOUR_ACCESS_TOKEN> \
--server-host storage.ucsc-cgl.org --skip-uuid-directory /app/dcc-metadata-indexer/redacted \
--skip-program TEST --skip-project TEST
```
The `USER_GROUP` environment variable is so that the output metadata and jsonl files get set to your current user:group instead of root. If you don't know your user:group, you can easily get it by substituting `<USER:GROUP>` with `$(stat -c '%u:%g' $HOME)`; this gets the `user:group` of your home folder. The `DATABASE_URL` indicates the url with the username, password, and all required information, to access the database to store invoicing information. `ES_SERVICE` indicates the domain name pointing to the elasticsearch service. In the example above, it is set to `localhost`, and the host port 9200 bound to the container 9200 port. This should allow the indexer to access elasticsearch. This may come in handy if using docker-compose and want to use a standalone elasticsearch image. 

## TODO

* need to add upload to Chris' script
* need to download all the metadata from the storage service
* use the above two to show end-to-end process, develop very simple cgi script to display table
* each workflow JSON needs a timestamp
* command line tool would merge the docs, taking the "level" at which each document will be merged in at
    * donor, sample, specimen
* need to refresh the schema document, there are more constraints to be added

In the future, look at adding options here for specifying where files should be merged:

    python merge.py --sample alignment:alignment1.json --sample alignment:alignment2.json --donor somatic_variant_calling:somatic.json

## Billing and database.
The metadata indexer was recently modified to deploy using docker as part of an
ongoing migration to docker. For compartmentalization and containerizing, the
work of generating invoices was moved intot he metadata indexer. To get this
working, you need to install everything from requirements.txt and then also set
the DATABASE_URL environment variable is set to point to a database URI in the
format expected by SQLAlchemy (see
http://docs.sqlalchemy.org/en/latest/core/engines.html).  You can point this to
any SQL database, but I will write down a few particular steps on getting this
runnning on AWS RDS using a postgreSQL instance.
* From the AWS management console, select the RDS tab and navigate to the
  instances tab. If you have a particular database instance already created you
  wish to set up with the indedxer, skip to GENERATING A DATABSE URI FROM AWS
* Select the Launch DB Instance tab, within it, select postgreSQL, provision it
  as you deem necessary for the purposes (cores, IOP/S, network connection,
  etc.)
## GENERATING A DATABSE URI FROM AWS
This assumes that you have located the database instance in AWS which you wish
to create a database URI for, after selecting it, click the see details
information panel in instance actions. a SQLALCHEMY DATABASE URI is composed of
a few parts, a username, a password, a port, a database name/hostname, and a
protocol.

Assuming you are setting up a postgreSQL database, your connection string will
begin with "postgresql://"

Working with a hypothetical database instance at the hostname
"test-db-endpoint.com" with port 5432 (this should likely not change if using
postgres, and you are connecting to a database named billings on that instance,
using a username "TESTUSER" and password "TESTPASSWORD", the SQLAlchemy
database URI for this would be the following

"postgresql://TESTUSER:TESTPASSWORD@test-db-endpoint.com:5432/billings"

More gonerally, they have form 
{PROTOCOL}{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE_NAME}
