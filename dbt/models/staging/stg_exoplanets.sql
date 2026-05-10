{{
  config(
    materialized='view',
    tags=['staging', 'exoplanet_core']
  )
}}

-- Staging layer: Read raw NASA Exoplanet Archive data from Snowflake
-- Source table:  EXOPLANETS_DB.RAW.RAW_EXOPLANET_DATA
--
-- Transformations applied here:
--   1. Column renaming to human-readable names
--   2. Type casting using try_cast() — invalid values become NULL instead of erroring
--   3. Deduplication: keep the most recently ingested record per planet

with raw_data as (
  select
    -- Planet identifiers
    kepler_name                               as planet_name,
    null                                      as planet_letter,
    null                                      as host_star_name,

    -- Planet physical properties
    try_cast(koi_prad  as float)              as planet_radius_earth_radii,
    try_cast(koi_smass as float)              as planet_mass_earth_masses,
    try_cast(koi_period as float)             as orbital_period_days,
    null                                      as semi_major_axis_au,
    try_cast(koi_teq   as float)              as equilibrium_temperature_k,

    -- Discovery information
    null                                      as discovery_method,
    null                                      as discovery_year,
    null                                      as publication_date,

    -- Host star properties
    null                                      as stellar_spectral_type,
    try_cast(koi_steff as float)              as stellar_effective_temperature_k,
    try_cast(koi_srad  as float)              as stellar_radius_solar_radii,
    null                                      as stellar_mass_solar_masses,
    null                                      as stellar_distance_pc,

    -- Observational flags
    null                                      as has_transit_data,
    null                                      as has_radial_velocity_data,

    -- Audit metadata
    ingestion_timestamp

  from {{ source('nasa_api', 'raw_exoplanet_data') }}
),

deduplicated as (
  select
    *,
    row_number() over (
      partition by planet_name
      order by ingestion_timestamp desc
    ) as rn
  from raw_data
  where planet_name is not null
)

select * exclude (rn)
from deduplicated
qualify rn = 1