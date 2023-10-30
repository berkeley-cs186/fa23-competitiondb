select
	p_brand,
	p_type,
	p_size,
	count(ps_suppkey) as supplier_cnt
from
	partsupp join part on partsupp.ps_partkey = part.p_partkey
where
	p_brand <> 'Brand#32'
    && (p_size = 32 || p_size = 16 || p_size = 31 || p_size = 2 || p_size = 47 || p_size = 13 || p_size = 30 || p_size = 22)
group by
	p_brand,
	p_type,
	p_size
	ps_suppkey
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;