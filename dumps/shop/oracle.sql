-- Shop data set (DUMP-1, DUMP-4): the same records in every dumps/shop/* file.
-- Names are quoted so they stay lower case like in the other dumps. Oracle stores '' as NULL,
-- so customers.name is nullable here and customer 6 has a NULL name (ORA-5).
CREATE TABLE "customers" (
    "id" NUMBER(10) PRIMARY KEY,
    "email" VARCHAR2(255 CHAR) NOT NULL UNIQUE,
    "name" VARCHAR2(4000 CHAR),
    "vip" NUMBER(1) DEFAULT 0 NOT NULL CHECK ("vip" IN (0, 1)),
    "balance" NUMBER(12,2),
    "created_at" TIMESTAMP(3) WITH TIME ZONE NOT NULL
);

CREATE TABLE "products" (
    "id" NUMBER(10) PRIMARY KEY,
    "sku" VARCHAR2(32 CHAR) NOT NULL UNIQUE,
    "name" VARCHAR2(4000 CHAR) NOT NULL,
    "price" NUMBER(10,2) NOT NULL,
    "weight_kg" BINARY_DOUBLE,
    "tags" CLOB CHECK ("tags" IS JSON),
    "image" BLOB,
    "available_from" DATE,
    "ship_cutoff" VARCHAR2(18),
    "external_id" RAW(16) NOT NULL
);

CREATE TABLE "orders" (
    "id" NUMBER(19) PRIMARY KEY,
    "customer_id" NUMBER(10) NOT NULL,
    "status" VARCHAR2(16 CHAR) NOT NULL,
    "ordered_at" TIMESTAMP(3) NOT NULL,
    "note" VARCHAR2(4000 CHAR),
    CONSTRAINT "orders_customer" FOREIGN KEY ("customer_id") REFERENCES "customers" ("id")
);

CREATE INDEX "orders_status" ON "orders" ("status");

CREATE TABLE "order_items" (
    "order_id" NUMBER(19) NOT NULL,
    "product_id" NUMBER(10) NOT NULL,
    "quantity" NUMBER(4) NOT NULL,
    "unit_price" NUMBER(10,2) NOT NULL,
    PRIMARY KEY ("order_id", "product_id"),
    CONSTRAINT "items_order" FOREIGN KEY ("order_id") REFERENCES "orders" ("id"),
    CONSTRAINT "items_product" FOREIGN KEY ("product_id") REFERENCES "products" ("id")
);

INSERT INTO "customers" VALUES (1, 'anna@example.com', 'Anna Kowalska', 1, 1250.50, TIMESTAMP '2024-01-15 09:30:00 +00:00');
INSERT INTO "customers" VALUES (2, 'bob@example.com', 'Bob O''Brien', 0, 0.00, TIMESTAMP '2024-02-01 14:00:00 +00:00');
INSERT INTO "customers" VALUES (3, 'zofia@example.com', 'Zofia Żółć-Gęślą', 0, -15.75, TIMESTAMP '2024-02-29 23:59:59.123 +00:00');
INSERT INTO "customers" VALUES (4, 'li@example.com', '李雷', 1, NULL, TIMESTAMP '2024-03-10 08:00:00 +00:00');
INSERT INTO "customers" VALUES (5, 'emoji@example.com', 'Emoji 🚀 User', 0, 99999.99, TIMESTAMP '2024-04-01 12:00:00 +00:00');
INSERT INTO "customers" VALUES (6, 'empty@example.com', NULL, 0, NULL, TIMESTAMP '2024-05-05 05:05:05 +00:00');

INSERT INTO "products" VALUES (1, 'SKU-001', 'Keyboard', 199.99, 0.95, '["office", "usb"]', HEXTORAW('89504e47'), DATE '2024-01-01', '16:30:00', HEXTORAW('6f1c2a3e1b2c4d5e8f90a1b2c3d4e5f6'));
INSERT INTO "products" VALUES (2, 'SKU-002', 'Mouse', 49.50, 0.12, '["office"]', NULL, DATE '2024-01-15', '16:30:00', HEXTORAW('0a9b8c7d6e5f4a3b9c2d1e0f9a8b7c6d'));
INSERT INTO "products" VALUES (3, 'SKU-003', 'Monitor 27"', 1299.00, 6.4, '{"hdr": true, "size": 27}', HEXTORAW('00ff10'), DATE '2024-02-01', '12:00:00', HEXTORAW('11111111222243338444555555555555'));
INSERT INTO "products" VALUES (4, 'SKU-004', 'Cable, 2m', 9.99, NULL, '[]', NULL, NULL, NULL, HEXTORAW('aaaaaaaabbbb4ccc8dddeeeeeeeeeeee'));
INSERT INTO "products" VALUES (5, 'SKU-005', 'Gift card', 100.00, 0, NULL, NULL, DATE '2024-12-01', '23:59:59', HEXTORAW('99999999888847778666555555555555'));

INSERT INTO "orders" VALUES (1, 1, 'paid', TIMESTAMP '2024-03-01 10:00:00', NULL);
INSERT INTO "orders" VALUES (2, 1, 'sent', TIMESTAMP '2024-03-05 11:30:00', 'leave at the door');
INSERT INTO "orders" VALUES (3, 2, 'new', TIMESTAMP '2024-03-06 09:15:00', NULL);
INSERT INTO "orders" VALUES (4, 3, 'paid', TIMESTAMP '2024-03-07 18:45:30.5', 'Proszę o fakturę; NIP 123');
INSERT INTO "orders" VALUES (5, 4, 'cancelled', TIMESTAMP '2024-03-08 07:00:00', NULL);
INSERT INTO "orders" VALUES (6, 5, 'new', TIMESTAMP '2024-03-09 20:00:00', 'gift; wrap it');

INSERT INTO "order_items" VALUES (1, 1, 1, 199.99);
INSERT INTO "order_items" VALUES (1, 2, 2, 49.50);
INSERT INTO "order_items" VALUES (2, 3, 1, 1299.00);
INSERT INTO "order_items" VALUES (3, 4, 3, 9.99);
INSERT INTO "order_items" VALUES (3, 2, 1, 49.50);
INSERT INTO "order_items" VALUES (4, 5, 2, 100.00);
INSERT INTO "order_items" VALUES (4, 1, 1, 189.99);
INSERT INTO "order_items" VALUES (5, 3, 1, 1299.00);
INSERT INTO "order_items" VALUES (6, 4, 10, 8.99);
