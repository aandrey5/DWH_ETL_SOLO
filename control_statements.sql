select source, count(*) from sellout

group by "source";

delete from sellout

select distinct("source") from sellout

select * from sellout limit 10
select  "source", sum(sale_sum) as sales, sum(purchase_sum) as purchase from sellout
group by  "source"
