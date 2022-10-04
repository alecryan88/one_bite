CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (
  file_name string,
  partition_date date,
  json_extract variant
   
);
