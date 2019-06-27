-- If this query returns no records, then we can feel decently confident
-- that we have indeed created the goldenbach_pairs table correctly.
-- We could also use a program like Mathematica to generate a dataset
-- to validate against.
select *
from num_facts.goldenbach_table_validation
where 
  primality_a <> 'Prime' 
  or primality_b <>'Prime'
  or a + b <> n
