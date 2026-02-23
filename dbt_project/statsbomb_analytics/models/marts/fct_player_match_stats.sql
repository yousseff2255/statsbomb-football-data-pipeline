with events as (
    select distinct * from {{ ref('stg_events') }}
),
matches as (
    select * from {{ ref('stg_matches') }}
),
players as (
    select * from {{ ref('stg_players') }}
),
teams as (
    select * from {{ ref('stg_teams') }}
),
event_types as (
    select * from {{ ref('stg_event_types') }}
)
select
    e.match_id,
    e.team_id,
    e.player_id,
    m.match_date,
    p.player_name,
    t.team_name,
    
    count(case when et.event_type_name = 'Pass' then 1 end) as passes_attempted,
    count(case when et.event_type_name = 'Pass' and e.is_pass_successful then 1 end) as passes_completed,
    
    count(case when et.event_type_name = 'Shot' then 1 end) as total_shots,
    sum(coalesce(e.shot_xg, 0)) as total_xg,
    sum(case when e.is_goal then 1 else 0 end) as total_goals,
    
    count(case when et.event_type_name = 'Dribble' then 1 end) as dribbles_attempted,
    sum(case when e.is_dribble_complete then 1 else 0 end) as dribbles_completed,
    
    count(case 
        when e.is_pass_successful and (e.end_location_x - e.location_x) > 10 
        then 1 
    end) as progressive_passes,
    
    count(case when e.counterpress then 1 end) as counterpressures
from events e
left join event_types et on e.event_type_sk = et.event_type_sk
left join matches m on e.match_id = m.match_id
left join players p on e.player_id = p.player_id
left join teams t on e.team_id = t.team_id
where e.player_id is not null
group by 1, 2, 3, 4, 5, 6