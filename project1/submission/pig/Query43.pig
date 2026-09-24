-- Query 4.3: Query 3.3 in Pig (Min/Max/Avg TransTotal per Age Range and Gender)
REGISTER '/opt/pig/lib/commons-collections3-3.2.2.jar';
rmf /user/Project1/output/pig43;

customers = LOAD '/user/Project1/data/customers' USING PigStorage(',')
    AS (id:int, name:chararray, age:int, gender:chararray, countryCode:int, salary:double);
transactions = LOAD '/user/Project1/data/transactions' USING PigStorage(',')
    AS (transId:int, custId:int, transTotal:double, transNumItems:int, transDesc:chararray);

-- age groups: [10,20) [20,30) [30,40) [40,50) [50,60) [60,70]  (70 included in last group)
cust = FOREACH customers GENERATE id, gender,
    (age < 20 ? '[10,20)' :
    (age < 30 ? '[20,30)' :
    (age < 40 ? '[30,40)' :
    (age < 50 ? '[40,50)' :
    (age < 60 ? '[50,60)' : '[60,70]'))))) AS ageRange;

trans = FOREACH transactions GENERATE custId, transTotal;

-- replicated (map-side) join: the small customers relation is loaded in memory
joined = JOIN trans BY custId, cust BY id USING 'replicated';

grouped = GROUP joined BY (cust::ageRange, cust::gender);
stats = FOREACH grouped GENERATE
    FLATTEN(group) AS (ageRange, gender),
    MIN(joined.trans::transTotal) AS minTransTotal,
    MAX(joined.trans::transTotal) AS maxTransTotal,
    ROUND_TO(AVG(joined.trans::transTotal), 2) AS avgTransTotal;

result = ORDER stats BY ageRange, gender;

STORE result INTO '/user/Project1/output/pig43' USING PigStorage(',');