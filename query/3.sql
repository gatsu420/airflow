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

, first_order as (
    select
        f.*
    from (
        select
            customer_id,
            date_part('year', order_date) as order_year,
            date_part('month', order_date) as order_month,
            row_number() over (
                partition by customer_id
                order by order_date
            ) as rn_order
        from fact_sales
    ) f
    join month_order on
        f.order_year = month_order.year_
        and f.order_month = month_order.month_
    where f.rn_order = 1
    and month_order.rn_ <= 6
)

, user_purchase as (
    select
        fact_sales.customer_id,
        sum(fact_sales.price) as total_purchase
    from fact_sales
    join first_order on
        fact_sales.customer_id = first_order.customer_id
    group by 1
)

, user_category as (
    select
        *,
        case
            when total_purchase <= 500                              then 'Low'
            when total_purchase > 500 and total_purchase <= 2000    then 'Medium'
            when total_purchase > 2000                              then 'High'
        else null end as purchase_category
    from user_purchase
)

select * from user_category
