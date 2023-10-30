select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp join supplier on partsupp.ps_suppkey = supplier.s_suppkey
	join nation on supplier.s_nationkey = nation.n_nationkey
where
	n_name = 'VIETNAM'
group by
	ps_partkey
order by
	value desc;