-- Test: Discovery years are within valid range
-- Purpose: Ensure discovery years are realistic (1992-2026)

select
  planet_name,
  discovery_year
from {{ ref('fct_exoplanets') }}
where discovery_year < 1992
  or discovery_year > 2026
  or discovery_year is null
