Select
        r.partition_date,
        v.country,
        v.state,
        v.city,
        v.price_level,
        u.user_type,
        count(*) as cnt,
        sum(review_score) as review_score_total

        
        
from {{ref('reviews')}} r 

left join {{ref('venues')}} v 
on v.venue_id = r.venue_id
and v.review_id = r.review_id

left join {{ref('users')}} u 
on u.user_id = r.user_id
and u.review_id = r.review_id

group by 1,2,3,4,5,6