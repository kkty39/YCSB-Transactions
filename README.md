<!--
Copyright (c) 2010 Yahoo! Inc., 2012 - 2016 YCSB contributors.
All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

YCSB+T
====================================

Background
-----

This project is based on version 0.17.0 of [YCSB](https://github.com/brianfrankcooper/YCSB/releases/tag/0.17.0), with additional support for transaction features as discussed in [Dr. Akon Dey](https://www.linkedin.com/in/akon-dey)'s 2014 IEEE [paper](https://ieeexplore.ieee.org/document/6818330).  

Introduction
-----

YCSB+T is an extension of the popular Yahoo! Cloud Serving Benchmark (YCSB), designed to measure the performance of cloud-based databases. This enhanced version, YCSB+T, introduces additional functionality for measuring the latency of transactions. Traditional YCSB primarily focused on throughput measurements, neglecting transactional latency. With YCSB+T, we aim to provide a more comprehensive performance evaluation tool by including latency metrics alongside throughput.  

Key Features
-----

YCSB+T introduces methods such as start(), commit(), and abort() for recording transactional latency. These methods enable users to precisely measure the time taken for each transaction, providing insights into system responsiveness and consistency.

The `ClosedEconomyWorkload` within the framework emulates a closed economy executing database operations such as create, read, update, and delete, among others, allowing for an extensive degree of customization. This setup features post-workload validation to ensure database state consistency, making it a versatile tool for analyzing database scalability and performance.

Links
-----

* YCSB official site: https://ycsb.site
* [Original YCSB project docs](https://github.com/brianfrankcooper/YCSB/wiki)
* [The original announcement from Yahoo!](https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/)

Getting Started
---------------

### Step 1: Data Loading with YCSB

First, we will use the original YCSB repository for data loading. This is more efficient as YCSB performs batch inserts and commits each batch instead of each individual record.

#### Download and Extract YCSB

```sh
curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar xfvz ycsb-0.17.0.tar.gz
cd ycsb-0.17.0
```

#### Set Up Database

Configure your database according to the README file found under the specific binding directory of your database in the extracted YCSB folder.

#### Load Data

- On Linux:
  
  ```sh
  bin/ycsb.sh load basic -P workloads/workloada
  ```

- On Windows:
  
  ```bat
  bin/ycsb.bat load basic -P workloads\workloada
  ```

### Step 2: Running Transactions with YCSB+T

After loading the data using the original YCSB, switch to YCSB+T for the running phase to explicitly coordinate transactions.

#### Clone and Build YCSB+T

```sh
curl -O --location https://github.com/kkty39/YCSB-Transactions
# Replace {module-name} with the specific module you're benchmarking, e.g., cloudspanner
mvn -pl site.ycsb:{module-name}-binding -am clean package
cd {module-name}/target/
tar -xzf ycsb-{module-name}-binding-0.17.0.tar.gz
cd ycsb-{module-name}-binding-0.17.0
```

#### Run Transactions

Ensure your database is set up for transactions as per the README in the YCSB+T binding directory.

- On Linux:
  
  ```sh
  ./bin/ycsb load {module-name} -P workloads/workloada
  ./bin/ycsb run {module-name} -P workloads/workloada
  ```

- On Windows:
  
  ```bat
  ./bin/ycsb.bat load {module-name} -P workloads\workloada
  ./bin/ycsb.bat run {module-name} -P workloads\workloada
  ```

### Additional Notes

- The `{module-name}` placeholder should be replaced with the name of the specific database binding you are testing, such as `cloudspanner`.
- For detailed configuration options and database-specific setup, refer to the README files within each binding's directory in both YCSB and YCSB+T repositories.

  Running the `ycsb` command without any argument will print the usage. 

  See https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload
  for a detailed documentation on how to run a workload.

  See https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for 
  the list of available workload properties.

### Known Limitations

- CockroachDB, MongoDB and JDBC drivers are still undergoing testing, so use with care


