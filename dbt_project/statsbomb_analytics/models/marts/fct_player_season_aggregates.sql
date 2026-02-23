with player_match_stats as (
    select * from {{ ref('fct_player_match_stats') }}
),

matches as (
    select * from {{ ref('stg_matches') }}
),

-- Extract season from match_date
-- Adjust the season logic based on your league (e.g., European leagues Aug-May)
player_matches_with_season as (
    select
        pms.*,
        m.competition_id,
        -- Create season: if month >= 8, season is current_year/next_year, else previous_year/current_year
        case 
            when extract(month from pms.match_date) >= 8 
            then extract(year from pms.match_date) || '/' || (extract(year from pms.match_date) + 1)
            else (extract(year from pms.match_date) - 1) || '/' || extract(year from pms.match_date)
        end as season
    from player_match_stats pms
    left join matches m on pms.match_id = m.match_id
)

select
    player_id,
    player_name,
    team_id,
    team_name,
    season,
    competition_id,
    
    -- Playing Time
    count(distinct match_id) as matches_played,
    -- Note: You'll need to add minutes_played to fct_player_match_stats
    -- For now, assuming 90 minutes per match as placeholder
    count(distinct match_id) * 90 as total_minutes_estimated,
    
    -- === PASSING METRICS ===
    sum(passes_attempted) as total_passes_attempted,
    sum(passes_completed) as total_passes_completed,
    
    -- Pass completion rate
    round(
        sum(passes_completed)::float / nullif(sum(passes_attempted), 0) * 100, 
        2
    ) as pass_completion_pct,
    
    sum(progressive_passes) as total_progressive_passes,
    
    -- Per 90 metrics
    round(
        sum(progressive_passes)::float / nullif(count(distinct match_id), 0),
        2
    ) as progressive_passes_per_match,
    
    -- === SHOOTING METRICS ===
    sum(total_shots) as total_shots,
    sum(total_goals) as total_goals,
    sum(total_xg) as total_xg,
    
    -- xG difference (finishing quality)
    round(sum(total_goals) - sum(total_xg), 2) as goals_minus_xg,
    
    -- Per 90 metrics (using match count as proxy for now)
    round(
        sum(total_shots)::float / nullif(count(distinct match_id), 0),
        2
    ) as shots_per_match,
    
    round(
        sum(total_goals)::float / nullif(count(distinct match_id), 0),
        2
    ) as goals_per_match,
    
    round(
        sum(total_xg)::float / nullif(count(distinct match_id), 0),
        2
    ) as xg_per_match,
    
    -- Shot conversion rate
    round(
        sum(total_goals)::float / nullif(sum(total_shots), 0) * 100,
        2
    ) as shot_conversion_pct,
    
    -- xG per shot (shot quality)
    round(
        sum(total_xg)::float / nullif(sum(total_shots), 0),
        3
    ) as xg_per_shot,
    
    -- === DRIBBLING METRICS ===
    sum(dribbles_attempted) as total_dribbles_attempted,
    sum(dribbles_completed) as total_dribbles_completed,
    
    -- Dribble success rate
    round(
        sum(dribbles_completed)::float / nullif(sum(dribbles_attempted), 0) * 100,
        2
    ) as dribble_success_pct,
    
    round(
        sum(dribbles_completed)::float / nullif(count(distinct match_id), 0),
        2
    ) as dribbles_completed_per_match,
    
    -- === DEFENSIVE METRICS ===
    sum(counterpressures) as total_counterpressures,
    
    round(
        sum(counterpressures)::float / nullif(count(distinct match_id), 0),
        2
    ) as counterpressures_per_match

from player_matches_with_season
group by 1, 2, 3, 4, 5, 6
-- Filter out players with minimal playing time
having count(distinct match_id) >= 3
order by season desc, total_goals desc