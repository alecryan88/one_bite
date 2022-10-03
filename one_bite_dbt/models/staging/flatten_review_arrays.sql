{{
    config(
        unique_key=['partition_date', 'review_id']
    )
}}


with duplicate_reviews as (
        Select 
                partition_date::date as partition_date,
                t.value:id::string as review_id,
                t.value as json_extract,
                row_number() over(partition by t.value:id order by partition_date) as rn

        from {{ source('reviews_json', 'reviews_json') }}, TABLE(FLATTEN(JSON_EXTRACT)) as t 

        {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        where partition_date::date between date('{{ var('run_start') }}') and date('{{ var('run_end') }}')

        {% endif %}
  )
  
Select

        partition_date,
        review_id,
        json_extract

from duplicate_reviews


WHERE rn = 1