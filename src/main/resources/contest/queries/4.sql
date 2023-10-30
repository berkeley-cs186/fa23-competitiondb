select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer join orders on customer.c_custkey = orders.o_custkey
	join lineitem on orders.o_orderkey = lineitem.l_orderkey
	join supplier on supplier.s_suppkey = lineitem.l_suppkey
	join nation on nation.n_nationkey = supplier.s_nationkey
	join region on region.r_regionkey = nation.n_regionkey
where
	r_name = 'EUROPE'
	&& o_orderdate >= DATE'1994-01-01'
	&& o_orderdate < DATE'1995-01-01'
group by
	n_name
order by
	revenue desc;