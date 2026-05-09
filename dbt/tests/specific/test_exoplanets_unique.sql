-- Test: Exoplanet names are unique in fact table
-- Purpose: Ensure no duplicate planets in the final fact table

select planet_name, count(*) as count
from {{ ref('fct_exoplanets') }}
group by planet_name
having count(*) > 1
