/**
 * Copyright (c) 2010 - 2016 Yahoo! Inc., 2016, 2019 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db;

import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.ClosedEconomyWorkload;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import site.ycsb.db.flavors.DBFlavor;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced
 * with YCSB. This class extends {@link DB} and implements the database
 * interface used by YCSB client.
 *
 * <br>
 * Each client will have its own instance of this class. This client is not
 * thread safe.
 *
 * <br>
 * This interface expects a schema <key> <field1> <field2> <field3> ... All
 * attributes are of type TEXT. All accesses are through the primary key.
 * Therefore, only one index on the primary key is needed.
 */
public class CockroachDBClient extends DB {

  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "db.url";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "db.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "db.passwd";

  /** The batch size for batched inserts. Set to >0 to use batching */
  public static final String DB_BATCH_SIZE = "db.batchsize";

  /** The JDBC fetch size hinted to the driver. */
  public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";

  public static final String JDBC_BATCH_UPDATES = "jdbc.batchupdateapi";

  /** The name of the property for the number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /** Default number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /** Representing a NULL value. */
  public static final String NULL_VALUE = "NULL";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "FIELD";

  /** Retry handling */
  private static final int MAX_RETRY_COUNT = 3;
  private static final String RETRY_SQL_STATE = "40001";
  private final Random rand = new Random();

  /** for validate */
  private static String standardValidate;

  private boolean sqlserver = false;
  private List<Connection> conns;
  private boolean initialized = false;
  private Properties props;
  private int jdbcFetchSize;
  private int batchSize;
  private boolean autoCommit;
  private boolean batchUpdates;
  private static final String DEFAULT_PROP = "";
  private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
  private long numRowsInBatch = 0;
  /** DB flavor defines DB-specific syntax and behavior for the
   * particular database. Current database flavors are: {default, phoenix} */
  private DBFlavor dbFlavor;

  /**
   * Ordered field information for insert and update statements.
   */
  private static class OrderedFieldInfo {
    private String fieldKeys;
    private List<String> fieldValues;

    OrderedFieldInfo(String fieldKeys, List<String> fieldValues) {
      this.fieldKeys = fieldKeys;
      this.fieldValues = fieldValues;
    }

    String getFieldKeys() {
      return fieldKeys;
    }

    List<String> getFieldValues() {
      return fieldValues;
    }
  }

  /**
   * For the given key, returns what shard contains data for this key.
   *
   * @param key Data key to do operation on
   * @return Shard index
   */
  private int getShardIndexByKey(String key) {
    int ret = Math.abs(key.hashCode()) % conns.size();
    return ret;
  }

  /**
   * For the given key, returns Connection object that holds connection to the
   * shard that contains this key.
   *
   * @param key Data key to get information for
   * @return Connection object
   */
  private Connection getShardConnectionByKey(String key) {
    return conns.get(getShardIndexByKey(key));
  }

  private void cleanupAllConnections() throws SQLException {
    for (Connection conn : conns) {
      if (!autoCommit) {
        conn.commit();
      }
      conn.close();
    }
  }

  /** Returns parsed int value from the properties if set, otherwise returns -1. */
  private static int getIntProperty(Properties props, String key) throws DBException {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      try {
        return Integer.parseInt(valueStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid " + key + " specified: " + valueStr);
        throw new DBException(nfe);
      }
    }
    return -1;
  }

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  @Override
  public void init() throws DBException {
    if (initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }
    props = getProperties();
    String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String driver = props.getProperty(DRIVER_CLASS);

    // TODO: check if curr workload is closed_economy_workload?
    // constructStandardQueriesAndFields(props);

    if (driver.contains("sqlserver")) {
      sqlserver = true;
    }

    this.jdbcFetchSize = getIntProperty(props, JDBC_FETCH_SIZE);
    this.batchSize = getIntProperty(props, DB_BATCH_SIZE);

    this.autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);
    this.batchUpdates = getBoolProperty(props, JDBC_BATCH_UPDATES, false);

    try {
      if (driver != null) {
        Class.forName(driver);
      }
      int shardCount = 0;
      conns = new ArrayList<Connection>(3);
      // for a longer explanation see the README.md
      // semicolons aren't present in JDBC urls, so we use them to delimit
      // multiple JDBC connections to shard across.
      final String[] urlArr = urls.split(";");
      for (String url : urlArr) {
        System.out.println("Adding shard node URL: " + url);
        Connection conn = DriverManager.getConnection(url, user, passwd);

        // Since there is no explicit commit method in the DB interface, all
        // operations should auto commit, except when explicitly told not to
        // (this is necessary in cases such as for PostgreSQL when running a
        // scan workload with fetchSize)
        conn.setAutoCommit(autoCommit);

        shardCount++;
        conns.add(conn);
      }

      System.out.println("Using shards: " + shardCount + ", batchSize:" + batchSize + ", fetchSize: " + jdbcFetchSize);

      cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();

      this.dbFlavor = DBFlavor.fromJdbcUrl(urlArr[0]);
    } catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBC driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }

    initialized = true;
    System.err.println("Successfully initialized...");
  }

  @Override
  public void start() throws DBException {
    super.start();
    try {
      conns.get(0).setAutoCommit(false);
      autoCommit = false;
    } catch (SQLException e) {
      e.printStackTrace();
      throw new DBException(e);
    }
  }

  @Override
  public void commit() throws DBException {
    super.commit();
    try {
      conns.get(0).commit();
    } catch (SQLException e) {
      e.printStackTrace();
      throw new DBException(e);
    }
  }

  @Override
  public void abort() throws DBException {
    super.abort();
    try {
      conns.get(0).rollback();
    } catch (SQLException e) {
      e.printStackTrace();
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (batchSize > 0) {
      try {
        // commit un-finished batches
        for (PreparedStatement st : cachedStatements.values()) {
          if (!st.getConnection().isClosed() && !st.isClosed() && (numRowsInBatch % batchSize != 0)) {
            st.executeBatch();
          }
        }
      } catch (SQLException e) {
        System.err.println("Error in cleanup execution. " + e);
        throw new DBException(e);
      }
    }

    try {
      cleanupAllConnections();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

  private PreparedStatement createAndCacheInsertStatement(StatementType insertType, String key)
      throws SQLException {
    String insert = dbFlavor.createInsertStatement(insertType, key);
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(insert);
    PreparedStatement stmt = cachedStatements.putIfAbsent(insertType, insertStatement);
    if (stmt == null) {
      return insertStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheReadStatement(StatementType readType, String key)
      throws SQLException {
    String read = dbFlavor.createReadStatement(readType, key);
    PreparedStatement readStatement = getShardConnectionByKey(key).prepareStatement(read);
    PreparedStatement stmt = cachedStatements.putIfAbsent(readType, readStatement);
    if (stmt == null) {
      return readStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType, String key)
      throws SQLException {
    String delete = dbFlavor.createDeleteStatement(deleteType, key);
    PreparedStatement deleteStatement = getShardConnectionByKey(key).prepareStatement(delete);
    PreparedStatement stmt = cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (stmt == null) {
      return deleteStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheUpdateStatement(StatementType updateType, String key)
      throws SQLException {
    String update = dbFlavor.createUpdateStatement(updateType, key);
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(update);
    PreparedStatement stmt = cachedStatements.putIfAbsent(updateType, insertStatement);
    if (stmt == null) {
      return insertStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheScanStatement(StatementType scanType, String key)
      throws SQLException {
    String select = dbFlavor.createScanStatement(scanType, key, sqlserver);
    PreparedStatement scanStatement = getShardConnectionByKey(key).prepareStatement(select);
    if (this.jdbcFetchSize > 0) {
      scanStatement.setFetchSize(this.jdbcFetchSize);
    }
    PreparedStatement stmt = cachedStatements.putIfAbsent(scanType, scanStatement);
    if (stmt == null) {
      return scanStatement;
    }
    return stmt;
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, 1, "", getShardIndexByKey(key));
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type, key);
      }
      readStatement.setString(1, key);

      /** Retry logic */
      // TODO: try removing retry logic
      int retryCount = 0;
      while (retryCount <= MAX_RETRY_COUNT) {
        ResultSet resultSet = null;
        try {
          resultSet = readStatement.executeQuery();
          if (!resultSet.next()) {
              return Status.NOT_FOUND;
          }
          if (result != null && fields != null) {
            for (String field : fields) {
              String value = resultSet.getString(field);
              result.put(field, new StringByteIterator(value));
            }
          }
          return Status.OK;
        } catch (SQLException e) {
          if (isRetryableError(e)) {
            // log retryable exception
            System.out.printf("Retryable exception occurred in processing read:\n    sql state = [%s]\n    message = [%s]\n    retry counter = %s\n", e.getSQLState(), e.getMessage(), retryCount);
            retryCount++;
            applyBackoffStrategy(retryCount);
            // TODO: do we need rollback for Read?
          } else {
            System.err.println("Non-retryable exceptioon ocuured in processing read: " + e.getMessage());
            e.printStackTrace();
            throw e;
          }
        } finally {
          // close result set
          if (resultSet != null) {
            try {
              resultSet.close();
            } catch (SQLException e) {
              System.err.println("Error closing ResultSet: " + e.getMessage());
              e.printStackTrace();
            }
          }
        }
      }
      // if all retry fail
      return Status.ERROR;
    } catch (SQLException e) {
      System.err.println("Error in processing read of table " + tableName + ": " + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, 1, "", getShardIndexByKey(startKey));
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type, startKey);
      }
      if (sqlserver) {
        scanStatement.setInt(1, recordcount);
        scanStatement.setString(2, startKey);
      } else {
        scanStatement.setString(1, startKey);
        scanStatement.setInt(2, recordcount);
      }
      ResultSet resultSet = scanStatement.executeQuery();
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }
          result.add(values);
        }
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing scan of table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      int numFields = values.size();
      OrderedFieldInfo fieldInfo = getFieldInfo(values);
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName,
          numFields, fieldInfo.getFieldKeys(), getShardIndexByKey(key));
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type, key);
      }
      int index = 1;
      for (String value: fieldInfo.getFieldValues()) {
        updateStatement.setString(index++, value);
      }
      updateStatement.setString(index, key);

      /** Retry logic */
      int retryCount = 0;
      while (retryCount <= MAX_RETRY_COUNT) {
        try {
          int result = updateStatement.executeUpdate();
          if (result == 1) {
            return Status.OK;
          }
          return Status.UNEXPECTED_STATE;
        } catch (SQLException e) {
          if (isRetryableError(e)) {
            // log retryable exception
            System.out.printf("retryable exception occurred in processing update:\n    sql state = [%s]\n    retry counter = %s\n", e.getSQLState(), retryCount);
            System.out.println("\nSTACK TRACE:");
            e.printStackTrace();
            retryCount++;
            // rollback
            // try {
            //   updateStatement.getConnection().rollback();
            // } catch (SQLException rollbackException) {
            //   System.err.println("error rolling back the transaction: " + rollbackException.getMessage());
            // }
            applyBackoffStrategy(retryCount);
          } else {
            System.err.println("Non-retryable exceptioon ocuured in processing update: " + e.getMessage());
            throw e;
          }
        }
      }
      // if all retries fail
      return Status.ERROR;
      // return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing update to table: " + tableName + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
      try {
          int numFields = values.size();
          OrderedFieldInfo fieldInfo = getFieldInfo(values);
          StatementType type = new StatementType(StatementType.Type.INSERT, tableName,
                  numFields, fieldInfo.getFieldKeys(), getShardIndexByKey(key));
          PreparedStatement insertStatement = cachedStatements.get(type);
          if (insertStatement == null) {
              insertStatement = createAndCacheInsertStatement(type, key);
          }
          insertStatement.setString(1, key);

          /** Retry logic */
          int retryCount = 0;
          while (retryCount <= MAX_RETRY_COUNT) {
              try {
                  // Add field values to the statement
                  int index = 2;
                  for (String value : fieldInfo.getFieldValues()) {
                      insertStatement.setString(index++, value);
                  }
                  // Using the batch insert API
                  if (batchUpdates) {
                      insertStatement.addBatch();
                      // Check for a sane batch size
                      if (batchSize > 0) {
                          // Commit the batch after it grows beyond the configured size
                          if (++numRowsInBatch % batchSize == 0) {
                              int[] results = insertStatement.executeBatch();
                              for (int r : results) {
                                  // Acceptable values are 1 and SUCCESS_NO_INFO (-2) from reWriteBatchedInserts=true
                                  if (r != 1 && r != -2) {
                                      return Status.ERROR;
                                  }
                              }
                              // If autoCommit is off, make sure we commit the batch
                              if (!autoCommit) {
                                  getShardConnectionByKey(key).commit();
                              }
                              return Status.OK;
                          } // else, the default value of -1 or a nonsense. Treat it as an infinitely large batch.
                      } // else, we let the batch accumulate
                      // Added element to the batch, potentially committing the batch too.
                      return Status.BATCHED_OK;
                  } else {
                      // Normal update
                      int result = insertStatement.executeUpdate();
                      // If we are not autoCommit, we might have to commit now
                      if (!autoCommit) {
                          // Let updates be batched locally
                          if (batchSize > 0) {
                              if (++numRowsInBatch % batchSize == 0) {
                                  // Send the batch of updates
                                  getShardConnectionByKey(key).commit();
                              }
                              return Status.OK;
                          } else {
                              // Commit each update
                              getShardConnectionByKey(key).commit();
                          }
                      }
                      if (result == 1) {
                          return Status.OK;
                      }
                  }

                  return Status.UNEXPECTED_STATE;
              } catch (SQLException e) {
                  if (isRetryableError(e)) {
                      // Log retryable exception
                      System.out.printf("Retryable exception occurred in processing insert:\n    sql state = [%s]\n    message = [%s]\n    retry counter = %s\n", e.getSQLState(), e.getMessage(), retryCount);
                      retryCount++;

                      // Rollback
                      try {
                          insertStatement.getConnection().rollback();
                      } catch (SQLException rollbackException) {
                          System.err.println("Error rolling back the transaction: " + rollbackException.getMessage());
                      }

                      applyBackoffStrategy(retryCount);
                  } else {
                      System.err.println("Non-retryable exception occurred in processing insert: " + e.getMessage());
                      e.printStackTrace();
                      throw e;
                  }
              }
          }

          // If all retries fail
          return Status.ERROR;
      } catch (SQLException e) {
          System.err.println("Error in processing insert to table: " + tableName + e);
          e.printStackTrace();
          return Status.ERROR;
      }
  }

  @Override
  public Status delete(String tableName, String key) {
    try {
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, 1, "", getShardIndexByKey(key));
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type, key);
      }
      deleteStatement.setString(1, key);
      int result = deleteStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + tableName + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public long validate() throws DBException{
    super.validate();
    long countedSum = 0L;
    String validateQuery = "SELECT SUM(field0::numeric) FROM usertable;";

    try (Connection conn = conns.get(0);
        PreparedStatement preparedStatement = conn.prepareStatement(validateQuery);
        ResultSet resultSet = preparedStatement.executeQuery()) {

        if (resultSet.next()) {
            countedSum = resultSet.getLong(1);
        } else {
            System.err.println("No result found for validation.");
        }

        if (resultSet.next()) {
            System.err.println("Expected exactly one row for validation.");
        }

        return countedSum;
    } catch (SQLException e) {
      System.err.println("Error in processing validate to table: " + e.getMessage());
      e.printStackTrace();
      throw new DBException(e);
    }
  }


  private OrderedFieldInfo getFieldInfo(Map<String, ByteIterator> values) {
    String fieldKeys = "";
    List<String> fieldValues = new ArrayList<>();
    int count = 0;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      fieldKeys += entry.getKey();
      if (count < values.size() - 1) {
        fieldKeys += ",";
      }
      fieldValues.add(count, entry.getValue().toString());
      count++;
    }

    return new OrderedFieldInfo(fieldKeys, fieldValues);
  }

  // TODO: maybe no need this method
  private static void constructStandardQueriesAndFields(Properties properties) {
    String validateTable = properties.getProperty(ClosedEconomyWorkload.TABLE_NAME_PROPERTY, ClosedEconomyWorkload.TABLE_NAME_PROPERTY_DEFAULT);
    String validateField = properties.getProperty(ClosedEconomyWorkload.FIELD_NAME, ClosedEconomyWorkload.DEFAULT_FIELD_NAME);
    // For PostgreSQL not int type 64, integer
    System.out.println("validate field: " + validateField);
    standardValidate = new StringBuilder().append("SELECT SUM(").append(validateField).append(") FROM ").append(validateTable).toString();
  }

  private boolean isRetryableError(SQLException e) {
    // Check if the exception is due to a retryable error
    return RETRY_SQL_STATE.equals(e.getSQLState());
  }

  private void applyBackoffStrategy(int retryCount) {
    int sleepMillis = (int) Math.pow(2, retryCount) * 100 + rand.nextInt(100);
    System.out.printf("Hit 40001 transaction retry error, sleeping %s milliseconds\n", sleepMillis);
    try {
      Thread.sleep(sleepMillis);
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
    }
  }
}
