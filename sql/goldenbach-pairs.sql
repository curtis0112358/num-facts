-- This statement will create a table containing a list of pairs of primes
-- satisfying the Goldenbach conjecture that for any even integer n >= 4
-- we can find a pair of two prime number p1 and p2 such that n = p1 + p2.
-- For example when n = 4, then we can let p1 = p2 = 2, and, thus, our golden
-- pair is (2, 2). For other numbers there could be a many valid choices. For
-- instance for 10 we have (3, 7), (5, 5), and (7, 3); However, (7, 3)
-- doesn't really add any value since addition is commutative, and so
-- we would like our result to just be [(3, 7), (5, 5)] or [(5, 5), (7, 3)].
create table goldenbach_pairs 
with (
  external_location = 's3://jamie-curtis/num_facts/goldenbach_pairs/'
  , format = 'parquet'
  , parquet_compression = 'snappy'
) as
select 
 n
 , transform(array_agg(a), x -> cast(row(x, n - x) as row(a BigInt, b BigInt))) as golden_pairs
from 
  (select n from evens where n >= 4)
    inner join 
  (select n as a from primes) on a <= n/2
where 
  n - a in (select n from primes)
group by n
order by n
