select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem join part on lineitem.l_partkey = part.p_partkey
where
	p_brand = 'Brand#25'
	&& p_container = 'WRAP BAG'
	&& l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p_partkey
	);