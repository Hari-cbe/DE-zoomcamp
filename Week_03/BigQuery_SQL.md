<h1> SQL QUERIES for BIG Queries </h1>
<h2><i>Creating External Table from data sources</i></h2>
CREATE OR REPLACE EXTERNAL TABLE dataset.table_name <br>
OPTIONS (<br>
format = 'fileformat',<br>
uris = ['gs://bucket_name/folder_name/file.format']);  <br>

<h2><i>Creating partation & Clustering </i></h2>

CREATE OR REPLACE TABLE dataset.new_table_name <br>
 PARTITION BY partition_by_column <br> 
 CLUSTER BY cluster_by_column AS <br>
 SELECT 
 *
 FROM
 dataset.from_table;