-- Shop data set (DUMP-1): the same records in every dumps/shop/* file.
-- Decimals are stored as text (DECIMAL TEXT, see sqlite_type_for) so no digit is lost.
PRAGMA foreign_keys = ON;

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name TEXT NOT NULL,
    vip BOOLEAN NOT NULL DEFAULT 0,
    balance DECIMAL TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    sku VARCHAR(32) NOT NULL UNIQUE,
    name TEXT NOT NULL,
    price DECIMAL TEXT NOT NULL,
    weight_kg REAL,
    tags JSON TEXT,
    image BLOB,
    available_from DATE,
    ship_cutoff TIME,
    external_id UUID TEXT NOT NULL
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers (id),
    status VARCHAR(16) NOT NULL,
    ordered_at DATETIME NOT NULL,
    note TEXT
);
CREATE INDEX orders_status ON orders (status);

CREATE TABLE order_items (
    order_id INTEGER NOT NULL REFERENCES orders (id),
    product_id INTEGER NOT NULL REFERENCES products (id),
    quantity SMALLINT NOT NULL,
    unit_price DECIMAL TEXT NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

INSERT INTO customers (id, email, name, vip, balance, created_at) VALUES
(1, 'anna@example.com', 'Anna Kowalska', 1, '1250.50', '2024-01-15T09:30:00+00:00'),
(2, 'bob@example.com', 'Bob O''Brien', 0, '0.00', '2024-02-01T14:00:00+00:00'),
(3, 'zofia@example.com', 'Zofia Żółć-Gęślą', 0, '-15.75', '2024-02-29T23:59:59.123+00:00'),
(4, 'li@example.com', '李雷', 1, NULL, '2024-03-10T08:00:00+00:00'),
(5, 'emoji@example.com', 'Emoji 🚀 User', 0, '99999.99', '2024-04-01T12:00:00+00:00'),
(6, 'empty@example.com', '', 0, NULL, '2024-05-05T05:05:05+00:00');

INSERT INTO products (id, sku, name, price, weight_kg, tags, image, available_from, ship_cutoff, external_id) VALUES
(1, 'SKU-001', 'Keyboard', '199.99', 0.95, '["office","usb"]', X'89504e47', '2024-01-01', '16:30:00', '6f1c2a3e-1b2c-4d5e-8f90-a1b2c3d4e5f6'),
(2, 'SKU-002', 'Mouse', '49.50', 0.12, '["office"]', NULL, '2024-01-15', '16:30:00', '0a9b8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d'),
(3, 'SKU-003', 'Monitor 27"', '1299.00', 6.4, '{"hdr":true,"size":27}', X'00ff10', '2024-02-01', '12:00:00', '11111111-2222-4333-8444-555555555555'),
(4, 'SKU-004', 'Cable, 2m', '9.99', NULL, '[]', NULL, NULL, NULL, 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'),
(5, 'SKU-005', 'Gift card', '100.00', 0, NULL, NULL, '2024-12-01', '23:59:59', '99999999-8888-4777-8666-555555555555');

INSERT INTO orders (id, customer_id, status, ordered_at, note) VALUES
(1, 1, 'paid', '2024-03-01 10:00:00', NULL),
(2, 1, 'sent', '2024-03-05 11:30:00', 'leave at the door'),
(3, 2, 'new', '2024-03-06 09:15:00', NULL),
(4, 3, 'paid', '2024-03-07 18:45:30.5', 'Proszę o fakturę; NIP 123'),
(5, 4, 'cancelled', '2024-03-08 07:00:00', NULL),
(6, 5, 'new', '2024-03-09 20:00:00', 'gift; wrap it');

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 1, '199.99'),
(1, 2, 2, '49.50'),
(2, 3, 1, '1299.00'),
(3, 4, 3, '9.99'),
(3, 2, 1, '49.50'),
(4, 5, 2, '100.00'),
(4, 1, 1, '189.99'),
(5, 3, 1, '1299.00'),
(6, 4, 10, '8.99');
