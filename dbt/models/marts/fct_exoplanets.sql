{{
  config(
    materialized='table',
    tags=['marts', 'exoplanet_facts']
  )
}}

-- Fact table: Core exoplanet properties and characteristics
-- Grain: One row per exoplanet
-- Purpose: Business-ready analytics on exoplanet discovery and properties
-- Note: indexes config removed — Snowflake manages query optimization automatically

with enriched as (
  select * from {{ ref('int_exoplanets_enriched') }}
)

select
  -- Surrogate key
  {{ dbt_utils.generate_surrogate_key(['planet_name', 'host_star_name']) }} as planet_id,

  -- Dimension keys
  planet_name,
  planet_letter,
  host_star_name,

  -- Planet physical properties
  planet_radius_earth_radii,
  planet_mass_earth_masses,
  planet_type,
  planet_density_earth_densities,

  -- Orbital characteristics
  orbital_period_days,
  semi_major_axis_au,

  -- Thermal properties
  equilibrium_temperature_k,
  habitability_classification,

  -- Discovery metadata
  discovery_method,
  discovery_year,
  publication_date,

  -- Host star properties
  stellar_spectral_type,
  stellar_effective_temperature_k,
  stellar_radius_solar_radii,
  stellar_mass_solar_masses,
  stellar_distance_pc,

  -- Observational flags
  has_transit_data,
  has_radial_velocity_data,

  -- Data quality metric
  case
    when planet_radius_earth_radii is not null
      and planet_mass_earth_masses is not null
      and equilibrium_temperature_k is not null
    then 'Complete'
    when planet_radius_earth_radii is not null
      or planet_mass_earth_masses is not null
    then 'Partial'
    else 'Minimal'
  end as data_completeness,

  -- Audit columns
  ingestion_timestamp,
  current_timestamp as updated_at

from enriched