{{
    config(
        unique_key=['partition_date', 'review_id']
    )
}}

Select  
        partition_date::date as partition_date,
        json_extract:id::string as review_id,
        json_extract:date::timestamp as review_timestamp,
        json_extract:deleted::boolean as deleted_reviewe,
        json_extract:featured::boolean as featured_review,
        json_extract:message::string as review_message,
        json_extract:score::float as review_score,
        json_extract:status::string as review_status ,
        json_extract:user:id::string as user_id,
        json_extract:venue:id::string as venue_id
        
from {{ ref('flatten_review_arrays') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where partition_date::date between date('{{ var('run_start') }}') and date('{{ var('run_end') }}')

{% endif %}