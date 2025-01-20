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
(1, 'Item 1', 100.50, '2024-01-19 10:00:00'),
(2, 'Item 2', 200.75, '2024-01-19 11:00:00'),
(3, 'Item 3', 150.25, '2024-01-19 12:00:00'),
(4, 'Item 4', 300.00, '2024-01-19 13:00:00'),
(5, 'Item 5', 250.80, '2024-01-19 14:00:00');

SELECT * FROM example_table;
EOF