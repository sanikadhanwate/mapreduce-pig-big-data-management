-- Query 4.3: age range + gender min, max, and average transaction total

REGISTER '/opt/pig/lib/commons-collections3-3.2.2.jar';
rmf /user/Project1/output/pig43;

customers = LOAD '/user/Project1/data/customers' USING PigStorage(',')
    AS (id:int, name:chararray, age:int, gender:chararray,
        countryCode:int, salary:double);

transactions = LOAD '/user/Project1/data/transactions' USING PigStorage(',')
    AS (transId:int, custId:int, transTotal:double,
        transNumItems:int, transDesc:chararray);

joined = JOIN transactions BY custId, customers BY id;

-- Join layout: transaction fields are $0-$4; customer fields are $5-$10.
enriched = FOREACH joined
    GENERATE (int)$7 AS age, (chararray)$8 AS gender,
             (double)$2 AS transTotal;

r10 = FILTER enriched BY age >= 10 AND age < 20;
r20 = FILTER enriched BY age >= 20 AND age < 30;
r30 = FILTER enriched BY age >= 30 AND age < 40;
r40 = FILTER enriched BY age >= 40 AND age < 50;
r50 = FILTER enriched BY age >= 50 AND age < 60;
r60 = FILTER enriched BY age >= 60 AND age <= 70;

l10 = FOREACH r10 GENERATE '[10,20)' AS ageRange, gender, transTotal;
l20 = FOREACH r20 GENERATE '[20,30)' AS ageRange, gender, transTotal;
l30 = FOREACH r30 GENERATE '[30,40)' AS ageRange, gender, transTotal;
l40 = FOREACH r40 GENERATE '[40,50)' AS ageRange, gender, transTotal;
l50 = FOREACH r50 GENERATE '[50,60)' AS ageRange, gender, transTotal;
l60 = FOREACH r60 GENERATE '[60,70]' AS ageRange, gender, transTotal;

labeled = UNION l10, l20, l30, l40, l50, l60;

by_group = GROUP labeled BY (ageRange, gender);

statistics = FOREACH by_group
    GENERATE FLATTEN(group) AS (ageRange:chararray, gender:chararray),
             MIN(labeled.transTotal) AS minTransTotal,
             MAX(labeled.transTotal) AS maxTransTotal,
             AVG(labeled.transTotal) AS avgTransTotal;

ordered = ORDER statistics BY ageRange, gender;

STORE ordered INTO '/user/Project1/output/pig43' USING PigStorage(',');