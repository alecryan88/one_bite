Select
        r.partition_date,
        case 
            when v.country_code = 'England' then 'GB'
            when  v.country_code = 'United States' then 'US'
            when  v.country_code = 'USA' then 'US'
            else v.country_code 
        end country_code,
        v.state_code,
        s.state_name,
        s.region_name,
        s.division_name,
        v.city_name,
        v.price_level,
        u.user_type,
        count(*) as review_count,
        sum(review_score) as review_score_total

        
        
from {{ref('reviews')}} r 

left join {{ref('venues')}} v 
on v.venue_id = r.venue_id
and v.review_id = r.review_id

left join {{ref('users')}} u 
on u.user_id = r.user_id
and u.review_id = r.review_id

left join {{ref('state_regions')}} s 
on s.state_code = v.state_code

{% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        where r.partition_date::date between date('{{ var('run_start') }}') and date('{{ var('run_end') }}')

{% endif %}

group by 1,2,3,4,5,6,7,8,9