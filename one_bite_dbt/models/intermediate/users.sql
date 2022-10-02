Select  
        partition_date::date as partition_date,
        json_extract:id::string as review_id,
        json_extract:user:id::string as user_id,
        json_extract:user:type::string as user_type,
        json_extract:user:createdAt::timestamp as user_created_at,
        json_extract:user:firstName::string as first_name,
        json_extract:user:lastName::string as last_name,
        json_extract:user:username::string as username,
        json_extract:user:email::string as email,
        json_extract:user:appleId::string as apple_id,
        json_extract:user:facebookId::string as facebook_id,
        json_extract:user:admin::boolean as admin,
        json_extract:user:banned::boolean as banned,
        json_extract:user:deleted::boolean as deleted
        
from {{ ref('flatten_review_arrays') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where partition_date::date between date('{{ var('run_start') }}') and date('{{ var('run_end') }}')

{% endif %}