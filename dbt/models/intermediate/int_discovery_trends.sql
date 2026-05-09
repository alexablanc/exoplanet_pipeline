{{
  config(
    materialized='view',
    tags=['intermediate', 'discovery_analytics']
  )
}}

-- Intermediate layer: Aggregate discovery trends and statistics
-- Provides insights into discovery methods, temporal trends, and stellar host characteristics

with staging as (
  select * from {{ ref('stg_exoplanets') }}
),

discovery_by_year as (
  select
    discovery_year,
    count(distinct planet_name) as planets_discovered,
    count(distinct host_star_name) as unique_stars,
    count(distinct discovery_method) as discovery_methods_used,
    avg(equilibrium_temperature_k) as avg_equilibrium_temp_k,
    avg(planet_radius_earth_radii) as avg_planet_radius,
    avg(orbital_period_days) as avg_orbital_period_days
  from staging
  where discovery_year is not null
  group by discovery_year
),

discovery_by_method as (
  select
    discovery_method,
    count(distinct planet_name) as total_planets_discovered,
    count(distinct host_star_name) as unique_stars,
    min(discovery_year) as first_discovery_year,
    max(discovery_year) as most_recent_discovery_year,
    avg(equilibrium_temperature_k) as avg_equilibrium_temp_k,
    avg(planet_radius_earth_radii) as avg_planet_radius
  from staging
  where discovery_method is not null
  group by discovery_method
),

discovery_by_star_type as (
  select
    stellar_spectral_type,
    count(distinct planet_name) as planets_around_star_type,
    count(distinct host_star_name) as unique_stars,
    avg(equilibrium_temperature_k) as avg_equilibrium_temp_k,
    avg(planet_radius_earth_radii) as avg_planet_radius,
    avg(orbital_period_days) as avg_orbital_period_days
  from staging
  where stellar_spectral_type is not null
  group by stellar_spectral_type
)

-- Return aggregated discovery trends
select
  'by_year' as trend_type,
  discovery_year::varchar as dimension,
  planets_discovered,
  unique_stars,
  discovery_methods_used,
  avg_equilibrium_temp_k,
  avg_planet_radius,
  avg_orbital_period_days
from discovery_by_year

union all

select
  'by_method' as trend_type,
  discovery_method as dimension,
  total_planets_discovered as planets_discovered,
  unique_stars,
  null as discovery_methods_used,
  avg_equilibrium_temp_k,
  avg_planet_radius,
  null as avg_orbital_period_days
from discovery_by_method

union all

select
  'by_star_type' as trend_type,
  stellar_spectral_type as dimension,
  planets_around_star_type as planets_discovered,
  unique_stars,
  null as discovery_methods_used,
  avg_equilibrium_temp_k,
  avg_planet_radius,
  avg_orbital_period_days
from discovery_by_star_type
