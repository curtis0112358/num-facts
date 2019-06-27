-- This statement defines a table which contains all of the primes between
-- 2 and 100,000. If a number is composite then it has a factor less than
-- its square root. So if a given number n is composite, then we can find an
-- integer 1 < k <= sqrt(n) such that the remainder of n when divide by k
-- is equal to zero (i.e. n % k = 0). Moreover, if n is prime then it will
-- always have a remainder greater than or equal to 1 for 1 < k <= sqrt(n).
-- This mean if we reduce by taking the minimum of all of the remainder
-- of n / k, starting with an initial minimum or 1, then prime numbers
-- will reduce to 1 and composite numbers will reduce to 0.
create table primes 
with (
  external_location='s3://jamie-curtis/num_facts/primes/'
  , format='parquet'
  , parquet_compression='snappy'
) as
select 
  n
  ,case 
     when n in (2, 3) then 'Prime'
     when reduce(
            transform(sequence(2, cast(floor(sqrt(n)) as integer)), m -> n % m)
              , cast(1 as bigint)
              ,  (s, x) -> array_min(array [s, x])
              , s -> s
           ) > 0 then 'Prime'
     else 'Composite'
   end as primality
  , floor(n/ 100) as block 
from unnest(sequence(2, 100000)) as t(n)
where 
  case 
     when n in (2, 3) then 'Prime'
     when reduce(
            transform(sequence(2, cast(floor(sqrt(n)) as integer)), m -> n % m)
              , cast(1 as bigint) 
              ,  (s, x) -> array_min(array [s, x])
              , s -> s
           ) > 0 then 'Prime'
     else 'Composite'
   end = 'Prime'
