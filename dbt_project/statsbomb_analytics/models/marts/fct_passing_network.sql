with events as (
    select distinct * from {{ ref('stg_events') }}
),

-- 1. Bring in the Event Types lookup table
event_types as (
    select distinct * from {{ ref('stg_event_types') }}
),

player_locations as (
    select
        e.match_id,
        e.team_id,
        e.player_id,
        avg(e.location_x) as avg_x,
        avg(e.location_y) as avg_y
    from events e
    -- 2. Join events to types so we can read the name
    left join event_types et on e.event_type_sk = et.event_type_sk
    where et.event_type_name = 'Pass' 
    group by 1, 2, 3
)

select
    e.match_id,
    e.team_id,
    e.player_id as passer_player_id,
    e.pass_recipient_id as recipient_player_id,
    
    count(*) as pass_count,
    
    -- Bring in the locations for the Passer (Start Node)
    p1.avg_x as passer_x,
    p1.avg_y as passer_y,
    
    -- Bring in the locations for the Recipient (End Node)
    p2.avg_x as recipient_x,
    p2.avg_y as recipient_y
    
from events e
-- Join location stats twice: once for passer, once for recipient
left join player_locations p1 on e.match_id = p1.match_id and e.player_id = p1.player_id
left join player_locations p2 on e.match_id = p2.match_id and e.pass_recipient_id = p2.player_id

where 
    e.is_pass_successful = true 
    and e.pass_recipient_id is not null
group by 1, 2, 3, 4, 6, 7, 8, 9
having count(*) > 2