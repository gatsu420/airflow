create table product_history (
	record_id text not null default gen_random_uuid(),
	product_id text not null,
	product_name text not null,
	category text not null,
	price decimal not null,
	valid_from timestamp,
	valid_to timestamp,
	primary key (record_id)
);

insert into product_history (product_id, product_name, category, price, valid_from, valid_to)
values
('prod_1', 'Kacang garuda', 'Makanan', 1000, current_timestamp, null),
('prod_2', 'Autan', 'Kebersihan', 1000, current_timestamp, null),
('prod_3', 'Indomie', 'Makanan', 2000, current_timestamp, null);
