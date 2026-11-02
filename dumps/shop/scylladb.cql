-- Shop data set (DUMP-1): the same records in every dumps/shop/* file. One statement per line
-- (the test loader splits on ';' at the end of a line); run inside the target keyspace.
CREATE TABLE customers (id int PRIMARY KEY, email text, name text, vip boolean, balance decimal, created_at timestamp);
CREATE TABLE products (id int PRIMARY KEY, sku text, name text, price decimal, weight_kg double, tags list<text>, image blob, available_from date, ship_cutoff time, external_id uuid);
CREATE TABLE orders (id bigint PRIMARY KEY, customer_id int, status text, ordered_at timestamp, note text);
CREATE TABLE order_items (order_id bigint, product_id int, quantity smallint, unit_price decimal, PRIMARY KEY (order_id, product_id));

INSERT INTO customers (id, email, name, vip, balance, created_at) VALUES (1, 'anna@example.com', 'Anna Kowalska', true, 1250.50, '2024-01-15 09:30:00+0000');
INSERT INTO customers (id, email, name, vip, balance, created_at) VALUES (2, 'bob@example.com', 'Bob O''Brien', false, 0.00, '2024-02-01 14:00:00+0000');
INSERT INTO customers (id, email, name, vip, balance, created_at) VALUES (3, 'zofia@example.com', 'Zofia Żółć-Gęślą', false, -15.75, '2024-02-29 23:59:59.123+0000');
INSERT INTO customers (id, email, name, vip, balance, created_at) VALUES (4, 'li@example.com', '李雷', true, null, '2024-03-10 08:00:00+0000');
INSERT INTO customers (id, email, name, vip, balance, created_at) VALUES (5, 'emoji@example.com', 'Emoji 🚀 User', false, 99999.99, '2024-04-01 12:00:00+0000');
INSERT INTO customers (id, email, name, vip, balance, created_at) VALUES (6, 'empty@example.com', '', false, null, '2024-05-05 05:05:05+0000');

INSERT INTO products (id, sku, name, price, weight_kg, tags, image, available_from, ship_cutoff, external_id) VALUES (1, 'SKU-001', 'Keyboard', 199.99, 0.95, ['office', 'usb'], 0x89504e47, '2024-01-01', '16:30:00', 6f1c2a3e-1b2c-4d5e-8f90-a1b2c3d4e5f6);
INSERT INTO products (id, sku, name, price, weight_kg, tags, image, available_from, ship_cutoff, external_id) VALUES (2, 'SKU-002', 'Mouse', 49.50, 0.12, ['office'], null, '2024-01-15', '16:30:00', 0a9b8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d);
INSERT INTO products (id, sku, name, price, weight_kg, tags, image, available_from, ship_cutoff, external_id) VALUES (3, 'SKU-003', 'Monitor 27"', 1299.00, 6.4, ['monitor'], 0x00ff10, '2024-02-01', '12:00:00', 11111111-2222-4333-8444-555555555555);
INSERT INTO products (id, sku, name, price, weight_kg, tags, image, available_from, ship_cutoff, external_id) VALUES (4, 'SKU-004', 'Cable, 2m', 9.99, null, [], null, null, null, aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee);
INSERT INTO products (id, sku, name, price, weight_kg, tags, image, available_from, ship_cutoff, external_id) VALUES (5, 'SKU-005', 'Gift card', 100.00, 0, null, null, '2024-12-01', '23:59:59', 99999999-8888-4777-8666-555555555555);

INSERT INTO orders (id, customer_id, status, ordered_at, note) VALUES (1, 1, 'paid', '2024-03-01 10:00:00+0000', null);
INSERT INTO orders (id, customer_id, status, ordered_at, note) VALUES (2, 1, 'sent', '2024-03-05 11:30:00+0000', 'leave at the door');
INSERT INTO orders (id, customer_id, status, ordered_at, note) VALUES (3, 2, 'new', '2024-03-06 09:15:00+0000', null);
INSERT INTO orders (id, customer_id, status, ordered_at, note) VALUES (4, 3, 'paid', '2024-03-07 18:45:30.500+0000', 'Proszę o fakturę; NIP 123');
INSERT INTO orders (id, customer_id, status, ordered_at, note) VALUES (5, 4, 'cancelled', '2024-03-08 07:00:00+0000', null);
INSERT INTO orders (id, customer_id, status, ordered_at, note) VALUES (6, 5, 'new', '2024-03-09 20:00:00+0000', 'gift; wrap it');

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 1, 1, 199.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 2, 2, 49.50);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (2, 3, 1, 1299.00);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (3, 4, 3, 9.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (3, 2, 1, 49.50);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (4, 5, 2, 100.00);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (4, 1, 1, 189.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (5, 3, 1, 1299.00);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (6, 4, 10, 8.99);
