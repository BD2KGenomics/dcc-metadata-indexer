
TSV_FILE="sample_metadata/20160514 - Sample Metadata Doc - Sheet1.tsv"
XL_FILE="sample_metadata//20160514 - Sample Metadata Doc.xlsx"
OUTPUT_DIR="output_test"

test2:
	python ./generate_metadata.py \
		-v \
		--biospecimenSchema biospecimen_flattened.json \
		--analysisSchema analysis_flattened.json \
		$(XL_FILE) \
	;

test:
	head -n 3 $(TSV_FILE) \
	> 1.tmp ;
	\
	python ./generate_metadata.py \
		-v \
		--biospecimenSchema biospecimen_flattened.json \
		--analysisSchema analysis_flattened.json \
		1.tmp $(XL_FILE) \
	;
	\

