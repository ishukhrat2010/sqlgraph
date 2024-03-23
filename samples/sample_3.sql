UPDATE sales.customer_orders
SET total_amount_this_month = SUM(orders.invoice_total) 
FROM sales.orders 
INNER JOIN calendar.current_month ON sales.orders.order_date BETWEEN calendar.current_month.start_date AND calendar.current_month.end_date
WHERE sales.orders.customer_id = sales.customer_orders.customer_id
;
