select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer join orders on customer.c_custkey = orders.o_custkey
	join lineitem on orders.o_orderkey = lineitem.l_orderkey
	join nation on customer.c_nationkey = nation.n_nationkey
where
	o_orderdate >= DATE'1993-10-01'
	&& o_orderdate < DATE'1994-01-01'
	&& l_returnflag = 'R'
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
LIMIT 20;