-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Initialize MySQL test data for Hive auto-create table E2E tests

-- Create test database if not exists
CREATE DATABASE IF NOT EXISTS test;
USE test;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS test_db_10;
DROP TABLE IF EXISTS test_db_11;
DROP TABLE IF EXISTS user_info;
DROP TABLE IF EXISTS order_info;

-- Create test_db_10 table with various MySQL data types
CREATE TABLE test_db_10 (
    `id` bigint(20) AUTO_INCREMENT NOT NULL,
    `name` varchar(100) DEFAULT NULL COMMENT 'User name',
    `age` int(10) DEFAULT NULL COMMENT 'User age',
    `sex` boolean DEFAULT NULL COMMENT 'User gender',
    `address` varchar(100) DEFAULT NULL COMMENT 'User address',
    `telephone` char(12) DEFAULT NULL COMMENT 'Phone number',
    `height` float DEFAULT NULL COMMENT 'Height in cm',
    `weight` double DEFAULT NULL COMMENT 'Weight in kg',
    `size` decimal(10,2) DEFAULT NULL COMMENT 'Size measurement',
    `ID_number` varchar(100) DEFAULT NULL COMMENT 'ID number',
    `date_time` date DEFAULT NULL COMMENT 'Date field',
    `ts` timestamp NULL COMMENT 'Timestamp field',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Test table for type conversion';

-- Insert test data into test_db_10
INSERT INTO test_db_10 (name, age, sex, address, telephone, height, weight, size, ID_number, date_time, ts) VALUES
('Alice Johnson', 25, true, '123 Main Street, New York', '123456789012', 165.5, 55.5, 10.25, 'ID001234567', '2023-01-01', '2023-01-01 10:00:00'),
('Bob Smith', 30, false, '456 Oak Avenue, Los Angeles', '234567890123', 175.0, 70.0, 15.50, 'ID002345678', '2023-01-02', '2023-01-02 11:00:00'),
('Charlie Brown', 35, true, '789 Pine Road, Chicago', '345678901234', 180.2, 80.8, 20.75, 'ID003456789', '2023-01-03', '2023-01-03 12:00:00'),
('Diana Prince', 28, false, '321 Elm Street, Houston', '456789012345', 160.0, 50.0, 8.90, 'ID004567890', '2023-01-04', '2023-01-04 13:00:00'),
('Eve Wilson', 32, true, '654 Maple Drive, Phoenix', '567890123456', 170.5, 65.3, 12.40, 'ID005678901', '2023-01-05', '2023-01-05 14:00:00'),
('Frank Miller', 45, false, '987 Cedar Lane, Philadelphia', '678901234567', 185.0, 90.2, 25.30, 'ID006789012', '2023-01-06', '2023-01-06 15:00:00'),
('Grace Lee', 29, true, '147 Birch Court, San Antonio', '789012345678', 162.8, 52.7, 9.85, 'ID007890123', '2023-01-07', '2023-01-07 16:00:00'),
('Henry Davis', 38, false, '258 Spruce Way, San Diego', '890123456789', 178.5, 75.4, 18.60, 'ID008901234', '2023-01-08', '2023-01-08 17:00:00'),
('Ivy Chen', 26, true, '369 Willow Path, Dallas', '901234567890', 158.2, 48.9, 7.75, 'ID009012345', '2023-01-09', '2023-01-09 18:00:00'),
('Jack Taylor', 42, false, '741 Aspen Street, San Jose', '012345678901', 182.1, 85.6, 22.90, 'ID010123456', '2023-01-10', '2023-01-10 19:00:00');

-- Create test_db_11 table for product information
CREATE TABLE test_db_11 (
    `id` bigint(20) AUTO_INCREMENT NOT NULL,
    `product_name` varchar(200) DEFAULT NULL COMMENT 'Product name',
    `price` decimal(10,2) DEFAULT NULL COMMENT 'Product price',
    `category` varchar(50) DEFAULT NULL COMMENT 'Product category',
    `in_stock` boolean DEFAULT NULL COMMENT 'Stock availability',
    `created_at` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT 'Creation time',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Product information table';

-- Insert test data into test_db_11
INSERT INTO test_db_11 (product_name, price, category, in_stock) VALUES
('MacBook Pro 16-inch', 2499.99, 'Electronics', true),
('Wireless Mouse', 29.99, 'Electronics', true),
('Mechanical Keyboard', 129.99, 'Electronics', false),
('4K Monitor 27-inch', 399.99, 'Electronics', true),
('USB-C Hub', 79.99, 'Electronics', true),
('Bluetooth Headphones', 199.99, 'Electronics', false),
('Webcam HD', 89.99, 'Electronics', true),
('Desk Lamp LED', 45.99, 'Furniture', true),
('Office Chair', 299.99, 'Furniture', false),
('Standing Desk', 599.99, 'Furniture', true);

-- Create user_info table
CREATE TABLE user_info (
    `user_id` bigint(20) AUTO_INCREMENT NOT NULL,
    `username` varchar(50) NOT NULL COMMENT 'Username',
    `email` varchar(100) DEFAULT NULL COMMENT 'Email address',
    `phone` varchar(20) DEFAULT NULL COMMENT 'Phone number',
    `status` tinyint(1) DEFAULT 1 COMMENT 'User status: 1=active, 0=inactive',
    `created_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT 'Account creation time',
    PRIMARY KEY (`user_id`),
    UNIQUE KEY `uk_username` (`username`),
    UNIQUE KEY `uk_email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='User information table';

-- Insert test data into user_info
INSERT INTO user_info (username, email, phone, status) VALUES
('john_doe', 'john.doe@example.com', '1234567890', 1),
('jane_smith', 'jane.smith@example.com', '2345678901', 1),
('bob_wilson', 'bob.wilson@example.com', '3456789012', 0),
('alice_johnson', 'alice.johnson@example.com', '4567890123', 1),
('charlie_brown', 'charlie.brown@example.com', '5678901234', 1),
('diana_prince', 'diana.prince@example.com', '6789012345', 0),
('eve_wilson', 'eve.wilson@example.com', '7890123456', 1);

-- Create order_info table
CREATE TABLE order_info (
    `order_id` bigint(20) AUTO_INCREMENT NOT NULL,
    `user_id` bigint(20) NOT NULL COMMENT 'User ID reference',
    `order_amount` decimal(12,2) NOT NULL COMMENT 'Order total amount',
    `order_status` varchar(20) DEFAULT 'PENDING' COMMENT 'Order status',
    `order_date` date NOT NULL COMMENT 'Order date',
    `created_at` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT 'Order creation time',
    PRIMARY KEY (`order_id`),
    KEY `idx_user_id` (`user_id`),
    KEY `idx_order_date` (`order_date`),
    KEY `idx_order_status` (`order_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Order information table';

-- Insert test data into order_info
INSERT INTO order_info (user_id, order_amount, order_status, order_date) VALUES
(1, 2499.99, 'COMPLETED', '2023-01-01'),
(2, 159.98, 'SHIPPED', '2023-01-02'),
(1, 399.99, 'PENDING', '2023-01-03'),
(3, 79.99, 'CANCELLED', '2023-01-04'),
(4, 545.97, 'COMPLETED', '2023-01-05'),
(2, 199.99, 'SHIPPED', '2023-01-06'),
(5, 89.99, 'PENDING', '2023-01-07'),
(1, 45.99, 'COMPLETED', '2023-01-08'),
(6, 299.99, 'CANCELLED', '2023-01-09'),
(4, 599.99, 'SHIPPED', '2023-01-10'),
(7, 129.99, 'PENDING', '2023-01-11'),
(3, 29.99, 'COMPLETED', '2023-01-12');

-- Show table structures for verification
DESCRIBE test_db_10;
DESCRIBE test_db_11;
DESCRIBE user_info;
DESCRIBE order_info;

-- Show record counts
SELECT 'test_db_10' as table_name, COUNT(*) as record_count FROM test_db_10
UNION ALL
SELECT 'test_db_11' as table_name, COUNT(*) as record_count FROM test_db_11
UNION ALL
SELECT 'user_info' as table_name, COUNT(*) as record_count FROM user_info
UNION ALL
SELECT 'order_info' as table_name, COUNT(*) as record_count FROM order_info;
