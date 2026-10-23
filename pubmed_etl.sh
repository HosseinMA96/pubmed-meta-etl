#!/bin/bash


source ./venv/bin/activate



# Configuration comes from environment variables, so no credentials live in this file.
# Example: export PUBMED_ETL_DB_USER=me PUBMED_ETL_DB_PASSWORD=... (only needed for the Postgres output)
gz_input="${PUBMED_ETL_GZ_INPUT:?set PUBMED_ETL_GZ_INPUT to the directory holding the .xml.gz files}"
parquet_output="${PUBMED_ETL_PARQUET_OUTPUT:?set PUBMED_ETL_PARQUET_OUTPUT to an empty output directory}"
cores="${PUBMED_ETL_CORES:-4}"
table_name="${PUBMED_ETL_TABLE:-public.pubmed_etl}"
user="${PUBMED_ETL_DB_USER:?set PUBMED_ETL_DB_USER}"
pas="${PUBMED_ETL_DB_PASSWORD:?set PUBMED_ETL_DB_PASSWORD}"

start_time=$(date +"%s")


python extractor.py -gz "$gz_input" -cores "$cores"


mid_time1=$(date +"%s")
elapsed_time_unzip=$((mid_time1 - start_time))

python parallel.py -xml "$gz_input" -parquet "$parquet_output" -cores "$cores" -wrap 0
python parallel.py -xml "$gz_input" -parquet "$parquet_output" -cores "$cores" -wrap 1


mid_time2=$(date +"%s")
elapsed_time_parse=$((mid_time2 - mid_time1))



spark-submit --master local[*] --jars './postgresql-42.5.2.jar' --driver-memory 220g  --conf "spark.local.dir=./logs" pyspark_parquet.py --tname "$table_name" --user "$user" --pas "$pas" --path "$parquet_output"

end_time=$(date +"%s")

elapsed_time_data=$((end_time - mid_time))

echo "Elapsed time for unzipping: elapsed_time_unzip seconds"
echo "Elapsed time for parsing: $elapsed_time_parse seconds"
echo "Elapsed time for data handling: $elapsed_time_data seconds"
