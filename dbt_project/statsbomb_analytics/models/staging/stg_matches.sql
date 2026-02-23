with source as (
    select * from {{ source('statsbomb', 'dim_match') }}
),

renamed as (
    select
        match_id,
        season_id,
        competition_id,
        match_date,
        home_team_id,
        away_team_id,
        home_score,
        away_score,
        case 
            when home_score > away_score then home_team_id
            when away_score > home_score then away_team_id
            else null 
        end as winner_team_id
    from source
)

select * from renamed