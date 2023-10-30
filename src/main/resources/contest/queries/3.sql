select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer join orders on customer.c_custkey = orders.o_custkey
	join lineitem on orders.o_orderkey = lineitem.l_orderkey
where
	c_mktsegment = 'FURNITURE'
	&& o_orderdate < DATE'1995-03-05'
	&& l_shipdate > DATE'1995-03-05'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
LIMIT 10;