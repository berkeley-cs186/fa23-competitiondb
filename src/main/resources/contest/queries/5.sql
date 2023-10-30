select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= DATE'1994-01-01'
	&& l_shipdate < DATE'1995-01-01' 
	&& l_discount <= 0.08 - 0.01
    && l_discount >= 0.08 + 0.01
	&& l_quantity < 25;