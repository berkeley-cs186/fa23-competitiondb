select
	s_name,
	count(*) as numwait
from
	supplier join lineitem on supplier.s_suppkey = lineitem.l_suppkey
	join orders on lineitem.l_orderkey = orders.o_orderkey
	join nation on supplier.s_nationkey = nation.n_nationkey
where
	o_orderstatus = 'F'
	&& l_receiptdate > l_commitdate
	&& n_name = 'KENYA'
group by
	s_name
order by
	numwait desc,
	s_name
LIMIT 100;