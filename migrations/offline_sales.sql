create table offline_sales (
	transaction_id text not null,
	store_id text not null,
	customer_id text not null,
	product_id text not null,
	quantity int not null,
	price decimal not null,
	transaction_date date,
	created_at timestamp with time zone,
	primary key (transaction_id)
);
