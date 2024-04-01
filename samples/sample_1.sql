-- Test1. Entire line is commented
select 1 as _id,
      getdate() as time_now,  /* Test 2:   
      this is a test for a multi-line comment
      block
      -- Test 3. False positive test for comment inside a commented block
      */TRUE as test_passed  -- Test 4. comment at the end of line
LIMIT 10
;
