select
	100.00 * sum(l_extendedprice * (1 - l_discount)) 
    / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem join part on lineitem.l_partkey = part.p_partkey
where
	l_shipdate >= DATE'1997-08-01'
	&& l_shipdate < DATE'1997-09-01';