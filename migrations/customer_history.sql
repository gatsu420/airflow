create table customer_history (
	record_id text not null default gen_random_uuid(),
	customer_id text not null,
	name text not null,
	email text not null,
	registration_date timestamp with time zone,
	valid_from timestamp with time zone,
	valid_to timestamp with time zone,
	primary key (record_id)
);
