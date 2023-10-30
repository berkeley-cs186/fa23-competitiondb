### Overview
Welcome to the CS186 contest where you can optimize RookieDB over a set of queries! You're free to make any change to your database but certain assumptions and rules are in place. Your goal is to optimize the total I/O count across all X queries.

**Assumptions:**
1. The queries are run sequentially (i.e., the lock manager and recovery managers are not being tested). 
2. None of the queries we'll be using are INSERT or CREATE SQL statements. Your query optimizer may choose to materialize intermediate outputs. 
3. RookieDB traditionally uses 4KB pages. However, we will be using 16KB pages.
4. Your database will have access to 256 MB of java memory. 


**Rules:** You may modify RookieDB as you please with some caveats. 

1. We will replace your buffer and disk manager with our implementation to ensure consistency when counting I/Os. 
2.  You may not add any intermediate tables by hand or create your own tables. 
3.  You are allowed to create indexes over any column and table. Keep in mind that RookieDB only supports generating indexes over one column.
4. We expect your query result to match the staff solution. 
5. You may change the buffer manager eviction policy. 

### TPCH
The Transaction Processing Performance Council (TPC) has a number of benchmarks for datasets; the tables we'll be looking at are from TPC-H, a decision support benchmark. The tables are meant to simulate real business data and the queries aim to help inform business decisions. 
**Tables:** 
- Table customer - c_custkey (int), c_name (string, 25), c_address (string, 40), c_nationkey (int), c_phone (string, 15), c_acctbal (float), c_mktsegment (string, 10), c_comment (string, 117)
- Table lineitem - l_orderkey (int), l_partkey (int), l_suppkey (int), l_linenumber (int), l_quantity (float), l_extendedprice (float), l_discount (float), l_tax (float), l_returnflag (string, 1), l_linestatus (string, 1), l_ship(date) (date), l_commit(date) (date), l_receipt(date) (date), l_shipinstruct (string, 25), l_shipmode (string, 10), l_comment (string, 44)
- Table nation - n_nationkey (int), n_name (string, 25), n_regionkey (int), n_comment (string, 152)
- Table orders - o_orderkey (int), o_custkey (int), o_orderstatus (string, 1), o_totalprice (float), o_order(date) (date), o_orderpriority (string, 15), o_clerk (string, 15), o_shippriority (int), o_comment (string, 79)
- Table part - p_partkey (int), p_name (string, 55), p_mfgr (string, 25), p_brand (string, 10), p_type (string, 25), p_size (int), p_container (string, 10), p_retailprice (float), p_comment (string, 23)
- Table partsupp (parts supplemental) - ps_partkey (int), ps_suppkey (int), ps_availqty (int), ps_supplycost (float), ps_comment (string, 199)
- Table region - r_regionkey (int), r_name (string, 25), r_comment (string, 152)
- Table supplier - s_suppkey (int), s_name (string, 25), s_address (string, 40), s_nationkey (int), s_phone (string, 15), s_acctbal (float), s_comment (string, 101)

Additionally, there are 4 sizes of table data. The size listed is for all tables combined. 
- Tiny: 32 KB total
- Small: 1MB total
- Medium: 3MB total
- Large: 5MB total

The tables and queries can be found under ```resources/contest/tables``` and ```resources/contest/queries```, respectively. Feel free to take a closer look at both the tables and queries to get a better sense of what's happening. 

### Code
*Note:* ```contest/ContestSetup.java``` should be the only file you need to modify. ```contest/ContestRunner.java``` is provided as a connivence when testing out your changes. The final contest is actually run through JUnit in ```test/contest/TestContest.java```.

**Building an Index:** If you would like to build an index over a particular column, go into ```contest/ContestSetup.java``` and add the table and column in ```INDICES_TO_BUILD```. For example, if I wanted to add an index over customer::c_custkey, I would add ```{"customer", "c_custkey"}``` as an array element. The contest runner will build the requested indexes before running any queries. Do note that this will count as part of your total IO cost. 

**Eviction Policy:** To change the eviction policy, replace the variable ```EVICTION_POLICY``` in ```contest/ContestSetup.java``` with the new eviction policy.

**ContestRunner.java:** This is a mock contest runner that you can use and modify. There are 4 static variables which you can change as you would like.
- ```PRINT_N_ROWS```: Print n rows of the query output.
- ```EXPORT_ROWS```: If you would like to serialize query output and schema, set this to true.
- ```WORKLOAD_SIZE_TO_RUN```: Corresponds to Tiny, Small, Medium, and Large database sizes. Change this to run the mock contest on a different size.
- ```EXPORT_PATH```: If you want to export the serialized output and schema, a file path you want to serialize the results to.


