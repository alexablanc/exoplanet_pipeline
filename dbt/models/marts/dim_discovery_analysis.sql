{{
  config(
    materialized='table',
    tags=['marts', 'discovery_dimension']
  )
}}

-- Dimension table: Discovery trends and statistics
-- Grain: One row per discovery trend aggregation
-- Purpose: Support discovery analytics and reporting
-- Note: indexes config removed — Snowflake manages query optimization automatically

with trends as (
  select * from {{ ref('int_discovery_trends') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['trend_type', 'dimension']) }} as discovery_trend_id,

  trend_type,
  dimension,
  planets_discovered,
  unique_stars,
  discovery_methods_used,

  -- Aggregate metrics
  avg_equilibrium_temp_k,
  avg_planet_radius,
  avg_orbital_period_days,

  -- Derived insights
  case
    when trend_type = 'by_year' and planets_discovered > 100  then 'High Discovery Rate'
    when trend_type = 'by_year' and planets_discovered between 50 and 100 then 'Moderate Discovery Rate'
    when trend_type = 'by_year' and planets_discovered < 50   then 'Low Discovery Rate'
    else null
  end as discovery_intensity,

  -- Audit columns
  current_timestamp as created_at,
  current_timestamp as updated_at

from trends