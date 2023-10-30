select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem join part on lineitem.l_partkey = part.p_partkey
	part
where
	(
		p_brand = 'Brand#52'
		&& (p_container = 'SM CASE' || p_container = 'SM BOX'  || p_container = 'SM BOX' || p_container = 'SM PACK' || p_container = 'SM PKG')
		&& l_quantity >= 6 && l_quantity <= 6 + 10
		&& p_size >= 1 
        && p_size <=  5
        && (l_shipmode = 'AIR' || l_shipmode = 'AIR REG')
		&& l_shipinstruct = 'DELIVER IN PERSON'
	)
	||
	(
		p_brand = 'Brand#32'
        && (p_container = 'MED BAG' || p_container = 'MED BOX' || p_container = 'MED PKG' || p_container = 'MED PACK')
		&& l_quantity >= 13 && l_quantity <= 13 + 10
        && p_size >= 1
        && p_size <= 10
		&& (l_shipmode = 'AIR' || l_shipmode = 'AIR REG')
		&& l_shipinstruct = 'DELIVER IN PERSON'
	)
	||
	(
		p_brand = 'Brand#51'
        && (p_container = 'LG CASE' || p_container = 'LG BOX' || p_container = 'LG PACK' || p_container = 'LG PKG')
		&& l_quantity >= 20 && l_quantity <= 20 + 10
        && p_size >= 1
        && p_size <= 15
        && (l_shipmode = 'AIR' || l_shipmode = 'AIR REG')
		&& l_shipinstruct = 'DELIVER IN PERSON'
	);