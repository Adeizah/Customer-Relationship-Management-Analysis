create database cap_stone;
use cap_stone;

/**
Creating the database tables and setting up the relationships
**/
create table accounts(
account varchar(100) primary key,
sector varchar(100),
year_established int,
revenue double,
employees int,
office_location varchar(100),
subsidiary_of varchar(100)
);

create table products(
product varchar(100) primary key,
series varchar(100),
sales_price int
);

create table sales_teams(
sales_agent varchar(100) primary key,
manager varchar(100),
regional_office varchar(100)
);

create table sales_pipeline(
opportunity_id varchar(100) primary key,
sales_agent varchar(100),
product varchar(100),
account varchar(100),
deal_stage varchar(50),
engage_date text,
close_date text,
close_value text
);

/*
Loading the data into the created tables
*/
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/accounts.csv"
into table accounts
fields terminated by ","
ignore 1 lines;

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/products.csv"
into table products
fields terminated by ","
ignore 1 lines;

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales_teams.csv"
into table sales_teams
fields terminated by ","
ignore 1 lines;

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales_pipeline.csv"
into table sales_pipeline
fields terminated by ","
ignore 1 lines;

/*
Cleaning the data in the populated tables tables
*/
-- Accounts table
select * from accounts;

update accounts
set sector = "technology" where sector = "technolgy";

-- Products table
select * from products;

update products
set product = "GTX PRO" where product = "GTXPRO";

-- Sales_teams table
select * from sales_teams;

-- Sales_pipeline table
describe sales_pipeline;
select * from sales_pipeline;
select * from sales_pipeline where sales_agent = null;

update sales_pipeline
set product = "GTX PRO" where product = "GTXPRO";

update sales_pipeline
set account = null where account = "";

update sales_pipeline
set engage_date = null where engage_date = "";

update sales_pipeline
set close_date = null where close_date = "";

set @close_value = (select min(close_value) from sales_pipeline);
update sales_pipeline
set close_value = null where close_value = @close_value;

alter table sales_pipeline
modify engage_date date,
modify close_date date,
modify close_value int;

alter table sales_pipeline
add constraint foreign key (sales_agent) references sales_teams(sales_agent),
add constraint foreign key (account) references accounts(account),
add constraint foreign key (product) references products(product)
;

select opportunity_id, sp.sales_agent as sales_agent, manager, regional_office, product, 
	sp.account as account, sector, deal_stage, engage_date, close_date, close_value
from sales_pipeline as sp left join sales_teams on sp.sales_agent = sales_teams.sales_agent
	left join accounts on sp.account = accounts.account;


/**
Querying the database
**/

/*
1. Sales Performance Analysis
*/
-- Total Deals and Won deals by Agents
drop temporary table if exists `Temp Total deals`;
create temporary table `Temp Total deals` as (
select sales_pipeline.sales_agent as sales_agent, count(opportunity_id) as `Total deals`
from sales_pipeline left join sales_teams
on sales_pipeline.sales_agent = sales_teams.sales_agent
group by sales_agent
);

drop temporary table if exists `Temp Won deals`;
create temporary table `Temp Won deals` as (
select sales_pipeline.sales_agent as sales_agent, count(opportunity_id) as `Won deals`
from sales_pipeline left join sales_teams
on sales_pipeline.sales_agent = sales_teams.sales_agent
where deal_stage = "Won"
group by sales_agent
);

select `Temp Total deals`.sales_agent as sales_agent, `Total deals`, `Won deals`
from `Temp Total deals` join `Temp Won deals` 
on `Temp Total deals`.sales_agent = `Temp Won deals`.sales_agent
order by sales_agent;

-- Total and Average Close Value by Agent
select sales_pipeline.sales_agent as sales_agent, sum(close_value) as `Total Close Value`, 
		avg(close_value) as `Average Close Value`
from sales_pipeline left join sales_teams
on sales_pipeline.sales_agent = sales_teams.sales_agent
group by sales_agent
order by sales_agent;

/*
2. Product Sales Analysis
*/
-- Total Units Sold and Close Value per Product
drop temporary table if exists `Temp Products Transactions`;
Create temporary table `Temp Products Transactions` as (
Select sales_pipeline.product as product, count(opportunity_id) as `Total Deals`
from sales_pipeline left join products
on sales_pipeline.product = products.product
group by product);

drop temporary table if exists `Temp Products Sold`;
Create temporary table `Temp Products Sold` as (
Select sales_pipeline.product as product, count(opportunity_id) as `Won Deals`
from sales_pipeline left join products
on sales_pipeline.product = products.product
where deal_stage = "Won"
group by product);

drop temporary table if exists `Temp Products Close Value`;
Create temporary table `Temp Products Close Value` as (
Select sales_pipeline.product as product, sum(close_value) as `Total Close Value`,
	avg(close_value) as `Average Close Value`
from sales_pipeline left join products
on sales_pipeline.product = products.product
where deal_stage = "Won"
group by product);

select `Temp Products Transactions`.product, `Total Deals`, `Won Deals`, `Total Close Value`, `Average Close Value`
from `Temp Products Transactions` join `Temp Products Sold`
on `Temp Products Transactions`.product = `Temp Products Sold`.product
join `Temp Products Close Value` 
on `Temp Products Transactions`.product = `Temp Products Close Value`.product;

/*
3. Sales Pipeline Analysis
*/
-- Transaction Distribution by Deal Stage
select deal_stage, count(opportunity_id) as count, sum(close_value) as sales
from sales_pipeline
group by deal_stage;

/*
4. Time/Period Analysis
*/
-- Transaction Distribution by Deal Stage
drop temporary table if exists `Temp Month Total Deals`;
create temporary table `Temp Month Total Deals` as (
select monthname(engage_date) as month, count(opportunity_id) as `Total Deals`
from sales_pipeline
group by month);

drop temporary table if exists `Temp Month Close Value`;
create temporary table `Temp Month Close Value` as (
select monthname(engage_date) as month, sum(close_value) as `Total Close Value`,
	avg(close_value) as `Average Close Value`
from sales_pipeline
group by month);

select `Temp Month Total Deals`.month, `Total Deals`, `Total Close Value`, `Average Close Value`
from `Temp Month Total Deals` join `Temp Month Close Value`
on `Temp Month Total Deals`.month = `Temp Month Close Value`.month;

/*
5. Account Analysis
*/
-- Total Transactions, Successful Transactions, Total and Average Close Values by Account
drop temporary table if exists `Temp Account Total Deals`;
create temporary table `Temp Account Total Deals` as (
select sales_pipeline.account, count(opportunity_id) as `Total Deals`
from sales_pipeline left join accounts
on sales_pipeline.account = accounts.account
group by account);

drop temporary table if exists `Temp Account Won Deals`;
create temporary table `Temp Account Won Deals` as (
select sales_pipeline.account, count(opportunity_id) as `Won Deals`
from sales_pipeline left join accounts
on sales_pipeline.account = accounts.account
where deal_stage = "Won"
group by account);

drop temporary table if exists `Temp Account Close Value`;
create temporary table `Temp Account Close Value` as (
select sales_pipeline.account, sum(close_value) as `Total Close Value`, 
	avg(close_value) as `Average Close Value`
from sales_pipeline left join accounts
on sales_pipeline.account = accounts.account
group by account);

select `Temp Account Total Deals`.account, `Total Deals`, `Won Deals`, `Total Close Value`, `Average Close Value`
from `Temp Account Total Deals` join `Temp Account Won Deals`
on `Temp Account Total Deals`.account = `Temp Account Won Deals`.account
join `Temp Account Close Value`
on `Temp Account Total Deals`.account = `Temp Account Close Value`.account;

-- Total Transactions, Successful Transactions, Total and Average Close Values by Sector
drop temporary table if exists `Temp Sector Total Deals`;
create temporary table `Temp Sector Total Deals` as (
select sector, count(opportunity_id) as `Total Deals`
from sales_pipeline left join accounts
on sales_pipeline.account = accounts.account
group by sector);

drop temporary table if exists `Temp Sector Won Deals`;
create temporary table `Temp Sector Won Deals` as (
select sector, count(opportunity_id) as `Won Deals`
from sales_pipeline left join accounts
on sales_pipeline.account = accounts.account
where deal_stage = "Won"
group by sector);

drop temporary table if exists `Temp Sector Close Value`;
create temporary table `Temp Sector Close Value` as (
select sector, sum(close_value) as `Total Close Value`, 
	avg(close_value) as `Average Close Value`
from sales_pipeline left join accounts
on sales_pipeline.account = accounts.account
group by sector);

select `Temp Sector Total Deals`.sector, `Total Deals`, `Won Deals`, `Total Close Value`, `Average Close Value`
from `Temp Sector Total Deals` join `Temp Sector Won Deals`
on `Temp Sector Total Deals`.sector = `Temp Sector Won Deals`.sector
join `Temp Sector Close Value`
on `Temp Sector Total Deals`.sector = `Temp Sector Close Value`.sector;

/*
6. Geography Analysis
*/
-- Total Transactions, Successful Transactions, Total and Average Close Values by Regional Office
drop temporary table if exists `Temp Location Total Deals`;
create temporary table `Temp Location Total Deals` as (
select regional_office, count(opportunity_id) as `Total Deals`
from sales_pipeline left join sales_teams
on sales_pipeline.sales_agent = sales_teams.sales_agent
group by regional_office);

drop temporary table if exists `Temp Location Won Deals`;
create temporary table `Temp Location Won Deals` as (
select regional_office, count(opportunity_id) as `Won Deals`
from sales_pipeline left join sales_teams
on sales_pipeline.sales_agent = sales_teams.sales_agent
where deal_stage = "Won"
group by regional_office);

drop temporary table if exists `Temp Location Close Value`;
create temporary table `Temp Location Close Value` as (
select regional_office, sum(close_value) as `Total Close Value`, 
	avg(close_value) as `Average Close Value`
from sales_pipeline left join sales_teams
on sales_pipeline.sales_agent = sales_teams.sales_agent
group by regional_office);

select `Temp Location Total Deals`.regional_office, `Total Deals`, `Won Deals`, `Total Close Value`, `Average Close Value`
from `Temp Location Total Deals` join `Temp Location Won Deals`
on `Temp Location Total Deals`.regional_office = `Temp Location Won Deals`.regional_office
join `Temp Location Close Value`
on `Temp Location Total Deals`.regional_office = `Temp Location Close Value`.regional_office;