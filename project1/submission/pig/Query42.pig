-- Query 4.2: country codes with customer count > 5000 or < 2000

REGISTER '/opt/pig/lib/commons-collections3-3.2.2.jar';
rmf /user/Project1/output/pig42;

customers = LOAD '/user/Project1/data/customers' USING PigStorage(',')
    AS (id:int, name:chararray, age:int, gender:chararray,
        countryCode:int, salary:double);

by_country = GROUP customers BY countryCode;

country_counts = FOREACH by_country
    GENERATE group AS countryCode, COUNT(customers) AS customerCount;

outliers = FILTER country_counts
    BY customerCount > 5000 OR customerCount < 2000;

STORE outliers INTO '/user/Project1/output/pig42' USING PigStorage(',');