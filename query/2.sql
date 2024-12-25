with month_order as (
	select
		*,
		row_number() over (
			order by year_ desc, month_ desc
		) rn_
	from (
		select
			distinct date_part('year', order_date) as year_,
			date_part('month', order_date) as month_
		from fact_sales
	) q
)

, sales as (
	select
		products.category,
		count(distinct fact_sales.order_id) as total_order
	from fact_sales
	left join products on
		fact_sales.product_id = products.product_id
	left join month_order on
		date_part('year', fact_sales.order_date) = month_order.year_
		and date_part('month', fact_sales.order_date) = month_order.month_
	where month_order.rn_ <= 3
	group by 1
)

, sales_order as (
	select
		*,
		row_number() over (
			order by total_order desc
		) as rank_total_order
	from sales
)

select * from sales_order
where rank_total_order <= 5
order by rank_total_order
