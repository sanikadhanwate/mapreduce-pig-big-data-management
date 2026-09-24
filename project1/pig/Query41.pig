-- Query 4.1: customer names having the least number of transactions

REGISTER '/opt/pig/lib/commons-collections3-3.2.2.jar';
rmf /user/Project1/output/pig41;

customers = LOAD '/user/Project1/data/customers' USING PigStorage(',')
    AS (id:int, name:chararray, age:int, gender:chararray,
        countryCode:int, salary:double);

transactions = LOAD '/user/Project1/data/transactions' USING PigStorage(',')
    AS (transId:int, custId:int, transTotal:double,
        transNumItems:int, transDesc:chararray);

cust = FOREACH customers GENERATE id, name;
trans = FOREACH transactions GENERATE custId;

-- Keeps customers with zero transactions.
cg = COGROUP cust BY id, trans BY custId;

counts = FOREACH cg
    GENERATE FLATTEN(cust.name) AS name, COUNT(trans) AS transCount;

all_counts = GROUP counts ALL;

min_count = FOREACH all_counts
    GENERATE MIN(counts.transCount) AS minTransCount;

with_min = CROSS counts, min_count;

candidates = FILTER with_min BY $1 == $2;

result = FOREACH candidates
    GENERATE $0 AS name, $1 AS transCount;

STORE result INTO '/user/Project1/output/pig41' USING PigStorage(',');