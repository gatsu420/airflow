select
	date_part('year', order_date) year_,
	date_part('month', order_date) month_,
	transaction_type,
	store_id,
	sum(price) as total_revenue
from fact_sales
group by 1, 2, 3, 4
order by 1, 2
