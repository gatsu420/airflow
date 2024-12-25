create table online_sales (
	order_id text not null,
	customer_id text not null,
	product_id text not null,
	quantity int not null,
	price decimal not null,
	order_date timestamp with time zone,
	created_at timestamp with time zone,
	primary key (order_id)
);
