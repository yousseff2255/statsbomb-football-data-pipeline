with source as (
    select * from {{ source('statsbomb', 'fact_events') }}
),

renamed as (
    select
        event_id,
        match_id,
        team_id,
        player_id,
        event_type_sk,
        period,
        minute,
        second,
        (minute * 60) + second as match_seconds,
        timestamp,
        location_x,
        location_y,
        end_location_x,
        end_location_y,
        shot_xg,
        pass_length,
        pass_outcome,
		pass_recipient_id,
		   possession_team_id, 
        duration,
        case when pass_outcome is null then true else false end as is_pass_successful,
        shot_outcome,
        case when shot_outcome = 'Goal' then true else false end as is_goal,
        dribble_outcome,
        case when dribble_outcome = 'Complete' then true else false end as is_dribble_complete,
        under_pressure,
        counterpress,
        play_pattern_id
    from source
)

select * from renamed