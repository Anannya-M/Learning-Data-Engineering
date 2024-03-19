show databases;
use world;
show tables;
select * from country;

SELECT 
    c.Code,
    c.Name AS country,
    c.Continent,
    city.Name AS city_name,
    city.District,
    c.Population
FROM
    Country c
        INNER JOIN
    city
WHERE
    c.Code = city.CountryCode
ORDER BY c.Population;

-- Using subqueries
SELECT 
    Name, population
FROM
    country
WHERE
    population = (SELECT 
            MAX(population)
        FROM
            country);

-- Using subquery with insert statement
CREATE TABLE top_countries (
    Name VARCHAR(100),
    population INT,
    SurfaceArea INT,
    GovernmentForm VARCHAR(255)
);
insert into top_countries (select Name,Population,SurfaceArea,GovernmentForm from country where population > 50000000);
SELECT 
    *
FROM
    top_countries
ORDER BY Population DESC;
drop table top_countries;

create database trigger_practice;
use trigger_practice;
create table customer(Id int, Name varchar(50), Age int);

-- Declaring a trigger
Delimiter //
create trigger age_verify
before insert on customer
for each row
if new.Age < 0 then set new.Age = 0;
end if; // 

insert into customer values
		(101, "Michael", 23),
        (102, "Ashlin", -23),
        (103, "Stuart", 24),
        (104, "James", -45);
select * from customer;

use world;
select * from country;

-- Using stored procedures
delimiter &&
create procedure topcountries4()
begin
select Name, Population, SurfaceArea, GovernmentForm from country where Population > 500000000;
End &&
delimiter ;

call topcountries4();

-- Using a stored procedure with IN statement
delimiter //
create procedure sort_countries(IN var int)
begin
select Name, Population, GovernmentForm from country 
ORDER BY Population DESC limit var;
end //
delimiter ; 
call sort_countries(10);
call sort_countries(3);

-- Creating Procedures for update command with parameters
use trigger_practice;
select * from customer;

delimiter //
create procedure updated_age(IN user_name varchar(25), IN age int)
begin
update customer set Age=age where Name=user_name and Id=104;
end //
delimiter ;
call updated_age("James",28);
call update_age("Ashlin", 35);
select * from customer;

delimiter //
create procedure count_employees(OUT total_cust int)
begin
select count(name) into total_cust from customer where Age < 35;
end //
delimiter ;
call count_employees(@young_emps);
select @young_emps AS young_customers;

-- Creating a view
use world;
select * from CountryLanguage;
select * from country;

create view country_details
AS
select c.Code, c.Name, cl.Language
from country c inner join CountryLanguage cl 
where c.Code=cl.CountryCode;

select * from country_details;

-- To show all the views
show full tables where TABLE_TYPE="VIEW";

--