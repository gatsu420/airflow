create table mart_sales_daily (
	order_date date not null,
	product_id text not null,
	product_name text not null,
	customer_name text not null,
	transaction_type text not null,
	store_id text not null,
	total_quantity int not null,
	total_price_paid decimal not null,
	total_order int not null,
	primary key (order_date, product_id)
);
