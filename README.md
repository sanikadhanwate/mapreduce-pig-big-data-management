# MapReduce Queries & Apache Pig — Big Data Management

A Hadoop-based Big Data Management project implementing analytical queries using
Java MapReduce and Apache Pig over large-scale customer and transaction datasets.

## 📌 Project Overview

This project demonstrates how distributed data-processing techniques can be used
to analyze large datasets using the Hadoop ecosystem.

The project includes:

- Synthetic customer and transaction data generation using Java
- Data storage and processing with HDFS
- Three Java MapReduce analytical queries
- Three Apache Pig queries
- Map-side processing, combiners, reducers, joins, grouping, filtering, and sorting
- Execution and validation on a large dataset containing 5 million transactions

---

## 🗂️ Dataset

Two datasets were generated for the project:

### Customers

Schema:

```text
CustomerID, Name, Age, Gender, CountryCode, Salary
```

### Transactions

Schema:

```
TransID, CustID, TransTotal, TransNumItems, TransDesc
Dataset Size
Dataset	Records
Customers	50,000
Transactions	5,000,000
Total records	5,050,000
```

The transaction dataset was approximately 280 MB and was loaded into HDFS
for distributed processing.

#### 🔹 Part 1 — Data Generation

GenerateData.java generates synthetic customer and transaction records.

The generated datasets were uploaded to HDFS:
```
/user/Project1/data/customers
/user/Project1/data/transactions
```
The data was tested first using a smaller dataset before running the
full 50,000-customer / 5,000,000-transaction workload.

#### 🔹 Part 2 — Java MapReduce Queries
= Query 3.1 — Customer Transaction Summary

The first MapReduce job joins the Customers and Transactions datasets
using CustomerID.

For every customer, the job reports:
```
CustomerID
Name
Salary
Number of Transactions
Total Transaction Sum
Minimum Number of Items
MapReduce Design
```

Two mappers process the datasets:

Customers
    ↓
CustomerMapper
    ↓
CustomerID → Customer information

Transactions
    ↓
TransactionMapper
    ↓
CustomerID → Transaction information

A combiner performs partial aggregation before the shuffle phase,
reducing the amount of intermediate data transferred to the reducer.

The reducer performs the customer-level join and calculates:

Transaction count
Total transaction amount
Minimum items per transaction
Sample Output
1,TLBGKwxixjdFExJks,3717.92,96,46836.98,1
2,ugAgUdRZHpTuHhSQuIda,8747.73,79,36845.38,1
3,nhcnKMKQJwVZvlzvZag,2508.28,111,51294.83,1
4,pwpPGRCZsJJjd,1230.97,126,63026.68,1
5,CujRPvCCEG,6910.10,107,59144.40,1
Execution Statistics
Map input records:       5,050,000
Map output records:      5,050,000
Combiner output records:   199,989
Reduce input groups:       50,000
Reduce output records:     50,000

- Query 3.2 — Country-Level Transaction Statistics

The second MapReduce job analyzes transaction statistics by CountryCode.

For every country code, the output contains:
```
CountryCode
Number of Customers
Minimum TransTotal
Maximum TransTotal
```
The query was implemented as a single MapReduce job using a combiner
to perform partial aggregation before the reducer.

Sample Output
1,4959,10.00,1000.00
2,4942,10.00,1000.00
3,4866,10.00,1000.00
4,5020,10.00,1000.00
5,5014,10.00,1000.00
6,5142,10.00,999.99
7,4936,10.00,1000.00
8,5168,10.00,1000.00
9,4964,10.00,1000.00
10,4989,10.00,1000.00
Execution Statistics
Map input records:       5,050,000
Map output records:      5,050,000
Combiner output records:        40
Reduce input groups:           10
Reduce output records:         10

- Query 3.3 — Age & Gender Analytics

The third MapReduce job divides customers into six age ranges:

[10,20)
[20,30)
[30,40)
[40,50)
[50,60)
[60,70]

Each age range is further divided by gender.

For every age/gender group, the job calculates:
```
Age Range
Gender
Minimum TransTotal
Maximum TransTotal
Average TransTotal
Sample Output
[10,20),female,10.00,1000.00,504.32
[10,20),male,10.00,1000.00,504.80
[20,30),female,10.00,1000.00,505.32
[20,30),male,10.00,1000.00,504.82
[30,40),female,10.01,1000.00,505.08
[30,40),male,10.00,1000.00,505.06
[40,50),female,10.00,1000.00,504.51
[40,50),male,10.00,1000.00,505.14
[50,60),female,10.00,999.99,506.10
[50,60),male,10.00,1000.00,505.04
[60,70],female,10.00,999.99,505.35
[60,70],male,10.00,1000.00,505.12
```
The job produced 12 output groups corresponding to the six age ranges
and two gender categories.

#### 🔹 Part 3 — Apache Pig Queries

The project also implements the required analytical tasks using
Apache Pig.

#### Query 4.1 — Customers With the Fewest Transactions

This Pig query identifies the customer(s) having the minimum number
of transactions.

The query accounts for the possibility of multiple customers sharing
the same minimum transaction count.
```
Output
IyQPQHKruTJtMHde,58
```
The output contains:

CustomerName, MinTransCount

#### Query 4.2 — Country Codes Based on Customer Count

This Pig query identifies country codes where the number of customers is:

> 5,000

or

< 2,000
Output
4,5020
5,5014
6,5142
8,5168

The output format is:

CountryCode, NumberOfCustomers

#### Query 4.3 — Age & Gender Transaction Analysis

The final Pig query implements the same analytical task as
MapReduce Query 3.3.

It groups customers by:

Age range
Gender

and calculates:

Minimum TransTotal
Maximum TransTotal
Average TransTotal
Output
[10,20),female,10.0,1000.0,504.32
[10,20),male,10.0,1000.0,504.8
[20,30),female,10.0,1000.0,505.32
[20,30),male,10.0,1000.0,504.82
[30,40),female,10.0,1000.0,505.08
[30,40),male,10.0,1000.0,505.06
[40,50),female,10.0,1000.0,504.51
[40,50),male,10.0,1000.0,505.14
[50,60),female,10.0,999.99,506.1
[50,60),male,10.0,1000.0,505.04
[60,70],female,10.0,999.99,505.35
[60,70],male,10.0,1000.0,505.12

The Pig implementation used operations including joins, grouping,
filtering, ordering, and combiners during execution.

## 🏗️ Project Structure
mapreduce-pig-big-data-management/
│

├── src/

│   ├── GenerateData.java

│   ├── Query31.java

│   ├── Query32.java

│   └── Query33.java

│

├── pig/

│   ├── Query41.pig

│   ├── Query42.pig

│   └── Query43.pig

│

├── results/

│   ├── query31.txt

│   ├── query32.txt

│   ├── query33.txt

│   ├── pig41.txt

│   ├── pig42.txt

│   └── pig43.txt

│

├── data/

│   └── # Generated datasets (excluded from Git)

│

├── build/

│   └── # Compiled Java classes (excluded from Git)

│

└── README.md

## 🛠️ Technologies
Java

Hadoop MapReduce

Apache Hadoop HDFS

Apache YARN

Apache Pig

Linux / Bash

Docker

VS Code

## ⚙️ Hadoop Processing Workflow
                 ┌─────────────────┐
                 │ Generate Data   │
                 │     Java        │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │      HDFS       │
                 │ Customers +     │
                 │ Transactions    │
                 └────────┬────────┘
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
     ┌─────────────────┐     ┌─────────────────┐
     │ Java MapReduce  │     │   Apache Pig    │
     │    Queries      │     │     Queries     │
     └────────┬────────┘     └────────┬────────┘
              │                       │
              └───────────┬───────────┘
                          ▼
                  ┌───────────────┐
                  │ HDFS Results  │
                  └───────────────┘
## 🚀 Key Concepts Demonstrated

Distributed file storage with HDFS

MapReduce programming model

Multiple-input MapReduce jobs

Mapper and Reducer design

Combiner optimization

Reduce-side joins

Aggregation and grouping

Filtering and ordering

Large-scale transaction processing

Apache Pig data-flow programming

Hadoop/YARN job execution

Working with multi-million-record datasets
