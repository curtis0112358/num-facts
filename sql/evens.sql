-- This statement creates a table containing even whole numbers
-- between 1 and 100,000.
create table num_facts.evens 
with (
  external_location = 's3://jamie-curtis/num_facts/evens'
  , format = 'parquet'
  , parquet_compression = 'snappy'
) as 
select 
n
, 'Even' as parity
, floor(n/ 100) as block 
from unnest(sequence(1, 100000)) as t(n) 
where t.n % 2 = 0
