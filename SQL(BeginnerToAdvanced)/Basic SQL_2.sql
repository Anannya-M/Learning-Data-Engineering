show databases;
use practice_sql;
show tables;

-- Creating another table 
CREATE TABLE customers (
    Cust_id INT PRIMARY KEY,
    cust_name VARCHAR(255),
    Age INT,
    Gender CHAR(10),
    Date_of_purchase DATE,
    Address VARCHAR(255),
    Item VARCHAR(25),
    Price FLOAT
);

show tables;

-- Inserting values
insert into customers values
		(101, "Joseph", 22, "Male", "2016-11-23", "Pune", "Vegetable", 80),
        (102, "Nilesh", 21, "Male", "2016-11-23", "New York", "softdrink", 800),
        (103, "Vipul", 33, "Male", "2016-11-23", "Miami", "candies", 620),
        (104, "Anubhav", 61, "Male", "2016-11-23", "Pune", "fruits", 250),
        (105, "Utkarsh", 45, "Male", "2016-11-23", "Miami", "snacks", 380),
        (106, "Ishan", 44, "Male", "2016-11-23", "New York", "potatoes", 60),
        (107, "Ankit", 52, "Male", "2016-11-23", "Chicago", "icecream", 220),
        (108, "Akshay", 36, "Male", "2016-11-23", "Pune", "coffee", 80),
        (109, "Akash", 28, "Male", "2016-11-23", "New York", "Vegetable", 150),
        (110, "Tanishq", 19, "Male", "2016-11-23", "Miami", "noodles", 400),
        (111, "Raghav", 24, "Male", "2016-11-23", "Detroit", "cups", 3400),
        (112, "Anand", 54, "Male", "2016-11-23", "Pune", "watermelon", 600),
        (113, "Amrit", 43, "Male", "2016-11-23", "Miami", "facewash", 340),
        (114, "Andrew", 18, "Male", "2016-11-23", "Boston", "shampoo", 950),
        (115, "Ricky", 26, "Male", "2016-11-23", "Detroit", "vanilla", 290);

select * from customers;
select cust_name, Item, Price from customers;

-- drop command deletes the whole table
drop table customers;
select * from customers;

-- truncate command is to delete all the records and the table will still be present
truncate table customers;
select * from customers;

-- alter command
alter table customers drop column Gender;
select * from customers;

-- update command
UPDATE customers 
SET 
    cust_name = 'James',
    Address = 'Boston'
WHERE
    Cust_id = 101;
select * from customers;

SELECT 
    Address, SUM(Price) AS total_price_per_city
FROM
    customers
GROUP BY Address
ORDER BY sum(Price) DESC;