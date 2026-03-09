CREATE DATABASE product_db;
CREATE DATABASE order_db;

\connect product_db;

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    price DOUBLE PRECISION
);

INSERT INTO products (name, price) VALUES
('Laptop', 1000.0),
('Phone', 500.0),
('Keyboard', 50.0);

\connect order_db;

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    product_id BIGINT,
    quantity INT
);

INSERT INTO orders (product_id, quantity) VALUES
(1, 2),
(2, 1);