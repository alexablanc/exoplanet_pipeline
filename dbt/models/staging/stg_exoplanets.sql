{{
  config(
    materialized='view',
    tags=['staging', 'exoplanet_core']
  )
}}

-- Staging layer: Ingest raw NASA Exoplanet Archive data
-- Minimal transformations: column standardization, type casting, deduplication
-- Source: NASA Exoplanet Archive API (ps_default table)

with raw_data as (
  select
    -- Planet identifiers (Kepler naming)
    kepler_name as planet_name,
    null as planet_letter,
    null as host_star_name,
    
    -- Planet properties
    koi_prad as planet_radius_earth_radii,
    koi_smass as planet_mass_earth_masses,
    koi_period as orbital_period_days,
    null as semi_major_axis_au,
    koi_teq as equilibrium_temperature_k,
    
    -- Discovery information
    null as discovery_method,
    null as discovery_year,
    null as publication_date,
    
    -- Host star properties
    null as stellar_spectral_type,
    koi_steff as stellar_effective_temperature_k,
    koi_srad as stellar_radius_solar_radii,
    null as stellar_mass_solar_masses,
    null as stellar_distance_pc,
    
    -- Observational properties
    null as has_transit_data,
    null as has_radial_velocity_data,
    
    -- Metadata
    current_timestamp as ingestion_timestamp
  
  from {{ source('nasa_api', 'raw_exoplanet_data') }}
)

-- Remove duplicates and nulls in critical fields
select
  *,
  row_number() over (partition by planet_name order by publication_date desc) as rn
from raw_data
where planet_name is not null
  and host_star_name is not null
qualify rn = 1
