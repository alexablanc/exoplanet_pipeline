{{
  config(
    materialized='view',
    tags=['intermediate', 'exoplanet_enrichment']
  )
}}

with staging as (
  select * from {{ ref('stg_exoplanets') }}
),

enriched as (
  select
    -- Core identifiers
    planet_name,
    planet_letter,
    host_star_name,
    
    -- Planet properties (original)
    planet_radius_earth_radii,
    planet_mass_earth_masses,
    orbital_period_days,
    semi_major_axis_au,
    equilibrium_temperature_k,
    
    -- Derived planet metrics
    case 
      when planet_mass_earth_masses > 0 and planet_radius_earth_radii > 0
        then planet_mass_earth_masses / (planet_radius_earth_radii * planet_radius_earth_radii * planet_radius_earth_radii)
      else null
    end as planet_density_earth_densities,
    
    -- Habitability zone classification (simplified)
    case
      when equilibrium_temperature_k between 250 and 320 then 'Habitable Zone'
      when equilibrium_temperature_k < 250 then 'Too Cold'
      when equilibrium_temperature_k > 320 then 'Too Hot'
      else 'Unknown'
    end as habitability_classification,
    
    -- Planet type classification
    case
      when planet_mass_earth_masses < 1.25 then 'Earth-sized'
      when planet_mass_earth_masses between 1.25 and 10 then 'Super-Earth/Mini-Neptune'
      when planet_mass_earth_masses between 10 and 300 then 'Neptune-sized'
      when planet_mass_earth_masses >= 300 then 'Jupiter-sized'
      else 'Unknown'
    end as planet_type,
    
    -- Discovery information
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
    
    -- Metadata
    ingestion_timestamp
  
  from staging
)

select * from enriched
