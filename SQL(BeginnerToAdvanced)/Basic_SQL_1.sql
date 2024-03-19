SHOW databases;
create database practice_sql;
use practice_sql;
show tables;
drop table emp_details;
CREATE TABLE emp_details (
    Id int PRIMARY KEY NOT NULL,
    Name VARCHAR(255) NOT NULL,
    Gender VARCHAR(50) NOT NULL,
    Age INT NOT NULL,
    dob DATE NOT NULL,
    City VARCHAR(255) NOT NULL,
    Salary FLOAT
);
show tables;
describe emp_details;

insert into emp_details values
			(1,"Jimmy", "Male", 35, "2005-05-30", "Chicago", 70000),
			(2,"Shane", "Male", 30, "1999-06-25", "Seattle", 55000),
            (3,"Mary", "Female", 28, "2009-03-10", "Boston", 62000),
            (4,"Dwayne", "Male", 37, "2011-07-12", "Austin", 57000),
            (5,"Sara", "Female", 32, "2017-10-27", "New York", 72000),
            (6,"Amy", "Female", 35, "2014-12-20", "Seattle", 80000);

select * from emp_details;

select city from emp_details;
select distinct city from emp_details;

select count(Name) from emp_details;
-- To make it readable use AS
select count(Name) AS total_employees from emp_details; 

-- To calculate the average salary of all the employees
select avg(salary) AS average_salary from emp_details; 

-- Selecting/ Displaying specific columns from the table
SELECT 
    Name, City, Salary
FROM
    emp_details;
    
-- Displaying only those which satisfy certail conditions using WHERE clause
SELECT 
    Name, City, Age, Salary
FROM
    emp_details
WHERE
    City = 'Seattle';
    
SELECT 
    Name, City, Age, Salary
FROM
    emp_details
WHERE
    Age < 35;

-- Using OR operator
SELECT 
    Name, City, Age, Salary
FROM
    emp_details
WHERE
    Age < 35 or City = "Seattle";
    
-- Using between clause
SELECT 
    Name, City, Age, Salary, dob
FROM
    emp_details
WHERE
    dob between "2000-01-01" and "2010-12-31";

-- Using group by clause to calculate the total salary in terms of gender
SELECT 
    Gender, sum(Salary) AS total_salary
FROM
    emp_details
group by Gender;

-- Sorting the table on the basis of salary
SELECT 
    *
FROM
    emp_details
ORDER BY Salary;

-- Some special select operations
SELECT 
    (20 + 30) AS addition,
    LENGTH('India') AS total_len,
    REPEAT('#', 5) AS repeated_character; -- # is repeated 5 times;
    
    
    






-- Displaying current date
select curdate();
select now();