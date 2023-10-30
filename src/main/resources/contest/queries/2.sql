select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part join partsupp on part.p_partkey = partsupp.ps_partkey
	join supplier on supplier.s_suppkey = partsupp.ps_suppkey
	join nation on supplier.s_nationkey = nation.n_nationkey
	join region on nation.n_regionkey = region.r_regionkey
where
	p_size = 47
	&& r_name = 'AFRICA'
	&& ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp join supplier on supplier.s_suppkey = partsupp.ps_suppkey
			join nation on supplier.s_nationkey = nation.n_nationkey
			join region on nation.n_regionkey = region.r_name
		where
			r_name = 'AFRICA'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
LIMIT 100;