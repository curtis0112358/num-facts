-- The purpose of this table is to validate that both components of the
-- Goldenbach pairs (golden pairs) are indeed prime. That is if we can
-- find any composites in the columns primality_a and primality_b, then
-- we will know that we have made a mistake. We can also validate n is always
-- equal the sum of a and b
create table num_facts.goldenbach_table_validation
with (
  external_location = 's3://jamie-curtis/num_facts/goldenbach_table_validation/'
  , format = 'parquet'
  , parquet_compression = 'snappy'
) as
select 
  n
  , r.a as a
  , case 
      when r.a in (2, 3) then 'Prime'
      when reduce(
             transform(sequence(2, cast(floor(sqrt(r.a)) as integer)), m -> r.a % m)
               , cast(1 as bigint)
               ,  (s, x) -> array_min(array [s, x])
               , s -> s
           ) > 0 then 'Prime'
      else 'Composite'
    end as primality_a
  , r.b as b 
  , case
      when r.b in (2, 3) then 'Prime'
      when reduce(
             transform(sequence(2, cast(floor(sqrt(r.b)) as integer)), m -> r.b % m)
               , cast(1 as bigint)
               ,  (s, x) -> array_min(array [s, x])
               , s -> s
           ) > 0 then 'Prime'
      else 'Composite' 
    end as primality_b
from  
  num_facts.goldenbach_pairs 
    cross join 
  unnest(golden_pairs) as t(r)
