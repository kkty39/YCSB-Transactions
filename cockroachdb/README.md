# Connect CockroachDB for YCSB
To enable YCSB to work with CockroachDB in Java, we need:
- JDBC driver
- PostgreSQL

## Getting Started
### 1. Start your database
This driver will connect to databases that use the JDBC protocol, please refer to Cockroach DB documentation on information on how to install, configure and start your system either on serverless cluster or local cluster.

### 2. Set up YCSB
You can clone the YCSB project and compile it to stay up to date with the latest changes. Or you can just build the single module for CockroachDB:

```sh
mvn -pl site.ycsb:cockroachdb-binding -am clean package
```

### 3. Configure your database and table.
After create a new SQL user and password, and follow essential steps to connect to database, you can create a new table with the following schema:

```sql
DROP TABLE IF EXISTS usertable;
CREATE TABLE usertable (
	YCSB_KEY VARCHAR(255) PRIMARY KEY,
	FIELD0 TEXT, FIELD1 TEXT,
	FIELD2 TEXT, FIELD3 TEXT,
	FIELD4 TEXT, FIELD5 TEXT,
	FIELD6 TEXT, FIELD7 TEXT,
	FIELD8 TEXT, FIELD9 TEXT
);
```
Hint: if you are working on a serverless cluster, you can pass SQL statements to SQL Shell. Refer to install CockroachDB if you prefer to work in command line.

### 4. Configure YCSB connection properties
You need to set the following connection configurations:

```sh
db.driver=org.postgresql.Driver
db.url=your_connection_string
db.user=your_user
db.passwd=your_password
```

Hint: for using multiple shards, specify the connection strings by using ";" as a delimiter.

### 5. Running a workload
Before you can actually run a workload, you need to load the data first. You can use the following command to load data:

```sh
./bin/ycsb.sh load cockroachdb -P path/to/your/db/properites -P path/to/your/workload
```
Then you can run the workload with the following command:

```sh
./bin/ycsb.sh run cockroachdb -P path/to/your/db/properites -P path/to/your/workload
```
Hint: you can add "-p" to specify the number of records to load, operations per second, threads, etc. Refer to YCSB for more details.

## Configuration Properties

```sh
db.driver=org.postgresql.Driver				# The JDBC driver class to use.
db.url=your_connection_string	# The Database connection URL.
db.user=admin								# User name for the connection.
db.passwd=admin								# Password for the connection.
db.batchsize=1000             # The batch size for doing batched inserts. Defaults to 0. Set to >0 to use batching.
jdbc.fetchsize=10							# The JDBC fetch size hinted to the driver.
jdbc.autocommit=true						# The JDBC connection auto-commit property for the driver. Change to false if you want to use transactions.
jdbc.batchupdateapi=false     # Use addBatch()/executeBatch() JDBC methods instead of executeUpdate() for writes (default: false)
```

Please refer to https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for all other YCSB core properties.

## JDBC Parameter to Improve Insert Performance

Some JDBC drivers support re-writing batched insert statements into multi-row insert statements. This technique can yield order of magnitude improvement in insert statement performance. To enable this feature:
- **db.batchsize** must be greater than 0.  The magniute of the improvement can be adjusted by varying **batchsize**. Start with a small number and increase at small increments until diminishing return in the improvement is observed.
- set **jdbc.batchupdateapi=true** to enable batching.
- set JDBC driver specific connection parameter in **db.url** to enable the rewrite as shown in the examples below:
  * Postgres [reWriteBatchedInserts=true](https://jdbc.postgresql.org/documentation/head/connect.html#connection-parameters) with `db.url=jdbc:postgresql://127.0.0.1:5432/ycsb?reWriteBatchedInserts=true`
