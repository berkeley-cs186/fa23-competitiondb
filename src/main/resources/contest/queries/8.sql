select
	l_shipmode
from
	orders join lineitem on orders.o_orderkey = lineitem.l_orderkey
where
	l_shipmode = 'REG AIR' || l_shipmode = 'MAIL'
	&& (l_commitdate < l_receiptdate
	&& l_shipdate < l_commitdate
	&& l_receiptdate >= DATE'1997-01-01'
	&& l_receiptdate < DATE'1998-01-01')
group by
	l_shipmode
order by
	l_shipmode;