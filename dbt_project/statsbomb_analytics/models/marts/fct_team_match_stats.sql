with player_stats as (
    select * 
    from {{ ref('fct_player_match_stats') }}
),
matches as (
    select *
    from {{ ref('stg_matches') }}
),
-- CTE for Possession Calculation
possession_data as (
    select
        match_id,
        possession_team_id as team_id,
        sum(duration) as possession_seconds
    from (select distinct * from {{ ref('stg_events') }})
    group by 1, 2
),
match_duration as (
    select
        match_id,
        sum(duration) as total_match_seconds
    from (select distinct * from {{ ref('stg_events') }})
    group by 1
),
team_aggs as (
    select
        match_id,
        team_id,
        sum(total_xg) as total_team_xg,
        sum(total_goals) as total_team_goals,
        sum(passes_completed) as total_passes,
        sum(total_shots) as total_shots,
        sum(counterpressures) as total_pressures
    from player_stats
    group by 1, 2
),
-- Get opponent for each team in each match
team_with_opponent as (
    select
        m.match_id,
        m.home_team_id as team_id,
        m.away_team_id as opponent_team_id,
        m.match_date,
        case
            when m.home_score > m.away_score then 'Win'
            when m.home_score = m.away_score then 'Draw'
            else 'Loss'
        end as match_result
    from matches m
    
    union all
    
    select
        m.match_id,
        m.away_team_id as team_id,
        m.home_team_id as opponent_team_id,
        m.match_date,
        case
            when m.away_score > m.home_score then 'Win'
            when m.home_score = m.away_score then 'Draw'
            else 'Loss'
        end as match_result
    from matches m
)
select
    two.match_id,
    two.team_id,
    two.match_date,
    t.team_name,
    two.opponent_team_id,
    two.match_result,
    -- Metrics
    ta.total_team_xg,
    ta.total_team_goals,
    oa.total_team_xg as opponent_xg,
    ta.total_team_xg - oa.total_team_xg as xg_difference,
    ta.total_passes,
    ta.total_shots,
    ta.total_pressures,
    -- Possession %
    round(
        (coalesce(pd.possession_seconds, 0) / nullif(md.total_match_seconds, 0)) * 100,
        2
    ) as possession_pct
from team_with_opponent two
left join team_aggs ta 
    on two.match_id = ta.match_id and two.team_id = ta.team_id
left join team_aggs oa 
    on two.match_id = oa.match_id and two.opponent_team_id = oa.team_id
left join {{ ref('stg_teams') }} t 
    on two.team_id = t.team_id
left join possession_data pd 
    on two.match_id = pd.match_id and two.team_id = pd.team_id
left join match_duration md 
    on two.match_id = md.match_id