
TSV_FILE="sample_metadata/sample.tsv"
XL_FILE="sample_metadata/sample.xlsx"
OUTPUT_DIR="output_test"
ACCESS_TOKEN=`cat ucsc-storage-client/accessToken`

test2:
	python ./generate_metadata.py \
		--inputMetadataSchema input_metadata.json \
		--metadataSchema metadata_schema.json \
		--awsAccessToken $(ACCESS_TOKEN) \
		--test \
		--skip-upload \
		$(XL_FILE) \
	;

test:
	head -n 3 $(TSV_FILE) \
	> 1.tmp ;
	\
	python ./generate_metadata.py \
		--inputMetadataSchema input_metadata.json \
		--metadataSchema metadata_schema.json \
		--awsAccessToken $(ACCESS_TOKEN) \
		--force-upload \
		1.tmp \
	;
	\
	rm -f 1.tmp ;
	\

merge:
	python ./merge_gen_meta.py \
		--directory output_metadata_7_20/ \
		--metadataSchema metadata_schema.json \
	;
	\

clean:
	rm -rf output_metadata ;
	\
