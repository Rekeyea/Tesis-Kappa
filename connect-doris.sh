#!/bin/bash

# Create user and grant privileges
docker exec -i kappa-doris-fe-1 mysql -h 127.0.0.1 -P 9030 -u root << EOF
CREATE USER 'kappa'@'%' IDENTIFIED BY 'kappa';
GRANT ALL ON *.* TO 'kappa'@'%';
EOF

# Create database, table and insert data
docker exec -i kappa-doris-fe-1 mysql -h 127.0.0.1 -P 9030 -u kappa -pkappa << EOF
CREATE DATABASE IF NOT EXISTS kappa;
USE kappa;

CREATE TABLE IF NOT EXISTS example_table (
    id INT,
    name VARCHAR(50),
    value DECIMAL(10,2),
    created_at DATETIME
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10;

INSERT INTO example_table VALUES 
(6, 'Item 1', 200.50, '2024-01-19 10:00:00'),
(7, 'Item 2', 300.75, '2024-01-19 11:00:00'),
(8, 'Item 3', 450.25, '2024-01-19 12:00:00'),
(9, 'Item 4', 500.00, '2024-01-19 13:00:00'),
(10, 'Item 5', 650.80, '2024-01-19 14:00:00');

SELECT * FROM example_table;
EOF