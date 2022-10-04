COPY INTO {{ params.schema }}.{{ params.table }} 
FROM (
    SELECT 
            METADATA$FILENAME as file_name,
            split_part(split_part(METADATA$FILENAME, '/', 1), '=', 2)::date as partition_date,
            $1 as json_extract
            
    FROM @{{params.stage}}
)