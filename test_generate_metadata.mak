
TSV_FILE="sample_metadata/sample.tsv"
XL_FILE="sample_metadata/sample.xlsx"
OUTPUT_DIR="output_test"

test2:
	python ./generate_metadata.py \
		-v \
		--metadataSchema metadata_flattened.json \
		$(XL_FILE) \
	;

test:
	head -n 3 $(TSV_FILE) \
	> 1.tmp ;
	\
	python ./generate_metadata.py \
		-v \
		--metadataSchema metadata_flattened.json \
		--skip-upload \
		1.tmp $(XL_FILE) \
	;
	\
	rm -f 1.tmp ;
	\
