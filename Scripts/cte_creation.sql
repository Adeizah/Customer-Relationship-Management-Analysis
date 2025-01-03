select count(*) from sales_teams;

with cte as (
select sales_pipeline.sales_agent as sales_agent, count(opportunity_id) as `Total deals`
from sales_pipeline left join sales_teams
on sales_pipeline.sales_agent = sales_teams.sales_agent
group by sales_agent
 union
select sales_pipeline.sales_agent as sales_agent, count(opportunity_id) as `Total deals`
from sales_pipeline right join sales_teams
on sales_pipeline.sales_agent = sales_teams.sales_agent
group by sales_agent)

select count(*) from cte;