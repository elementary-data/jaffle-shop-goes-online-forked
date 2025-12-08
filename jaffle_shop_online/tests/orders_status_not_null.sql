-- Test to identify orders with null status values
-- This test will fail if any orders have null status
select * from elementary_tests.jaffle_shop.orders where status is null