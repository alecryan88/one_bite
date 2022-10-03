
{{
    config(
        unique_key=['partition_date', 'review_id']
    )
}}

Select 
        partition_date::date as partition_date,
        json_extract:id::string as review_id,
        json_extract:venue:id::string as venue_id,
        json_extract:venue:name::string as venue_name,
        json_extract:venue:address1::string as address,
        json_extract:venue:address2::string as address_more,
        json_extract:venue:city::string as city_name,
        json_extract:venue:country::string as country_code,
        json_extract:venue:state::string as state_code,
        json_extract:venue:timeZone::string as timezone,
        json_extract:venue:priceLevel::string as price_level,
        json_extract:venue:zip::string as venue_zip,
        json_extract:venue:type::string as venue_type,
        json_extract:venue:phoneNumber::string as phone_number
        
from {{ ref('flatten_review_arrays') }}

{% if is_incremental() %}

  where partition_date::date between date('{{ var('run_start') }}') and date('{{ var('run_end') }}')

{% endif %}