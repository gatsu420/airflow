create table fact_sales (
	order_id text not null,
	customer_id text not null,
	customer_name text not null,
	product_id text not null,
	product_name text not null,
	quantity int not null,
	price decimal not null,
	transaction_type text not null,
	store_id text not null,
	order_date date,
	primary key (order_id)
);
