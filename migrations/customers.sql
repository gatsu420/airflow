create table customers (
	customer_id text not null,
	name text not null,
	email text not null,
	registration_date timestamp with time zone,
	primary key (customer_id)
);
