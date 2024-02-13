/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
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
package site.ycsb.db.cloudspanner;

import com.google.cloud.spanner.*;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Statement.Builder;
import com.google.common.base.Joiner;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.ClosedEconomyWorkload;
import site.ycsb.workloads.CoreWorkload;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;

/**
 * YCSB Client for Google's Cloud Spanner.
 */
public class CloudSpannerClient extends DB {

  /**
   * The names of properties which can be specified in the config files and flags.
   */
  public static final class CloudSpannerProperties {
    private CloudSpannerProperties() {}

    /**
     * The Cloud Spanner database name to use when running the YCSB benchmark, e.g. 'ycsb-database'.
     */
    static final String DATABASE = "cloudspanner.database";
    /**
     * The Cloud Spanner instance ID to use when running the YCSB benchmark, e.g. 'ycsb-instance'.
     */
    static final String INSTANCE = "cloudspanner.instance";
    /**
     * Choose between 'read' and 'query'. Affects both read() and scan() operations.
     */
    static final String READ_MODE = "cloudspanner.readmode";
    /**
     * The number of inserts to batch during the bulk loading phase. The default value is 1, which means no batching
     * is done. Recommended value during data load is 1000.
     */
    static final String BATCH_INSERTS = "cloudspanner.batchinserts";
    /**
     * Number of seconds we allow reads to be stale for. Set to 0 for strong reads (default).
     * For performance gains, this should be set to 10 seconds.
     */
    static final String BOUNDED_STALENESS = "cloudspanner.boundedstaleness";

    // The properties below usually do not need to be set explicitly.

    /**
     * The Cloud Spanner project ID to use when running the YCSB benchmark, e.g. 'myproject'. This is not strictly
     * necessary and can often be inferred from the environment.
     */
    static final String PROJECT = "cloudspanner.project";
    /**
     * The Cloud Spanner host name to use in the YCSB run.
     */
    static final String HOST = "cloudspanner.host";
    /**
     * Number of Cloud Spanner client channels to use. It's recommended to leave this to be the default value.
     */
    static final String NUM_CHANNELS = "cloudspanner.channels";
    /**
     * Choose between 'update' and 'query'. Affects only update() operations.
     */
    static final String UPDATE_MODE = "cloudspanner.updatemode";
  }

  private static int fieldCount;

  private static boolean queriesForReads;

  private static boolean queriesForUpdates;

  private static int batchInserts;

  private static TimestampBound timestampBound;

  private static String standardQuery;

  private static String standardScan;

  private static String standardValidate;

  private static final ArrayList<String> STANDARD_FIELDS = new ArrayList<>();

  private static final String PRIMARY_KEY_COLUMN = "id";

  private static final Logger LOGGER = Logger.getLogger(CloudSpannerClient.class.getName());

  // Static lock for the class.
  private static final Object CLASS_LOCK = new Object();

  // Single Spanner client per process.
  private static Spanner spanner = null;

  // Single database client per process.
  private static DatabaseClient dbClient = null;

  // Create the transaction manager in start() before operations starts.
  private TransactionManager transactionManager = null;

  // Used for executing operations in transactions.
  private TransactionContext tx = null;

  // Buffered mutations on a per object/thread basis for batch inserts.
  // Note that we have a separate CloudSpannerClient object per thread.
  private final ArrayList<Mutation> bufferedMutations = new ArrayList<>();

  private static void constructStandardQueriesAndFields(Properties properties) {
    String table = properties.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    final String fieldprefix = properties.getProperty(CoreWorkload.FIELD_NAME_PREFIX,
                                                      CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
    String validateTable = properties.getProperty(ClosedEconomyWorkload.TABLE_NAME_PROPERTY,
                                                  ClosedEconomyWorkload.TABLE_NAME_PROPERTY_DEFAULT);
    String validateField = properties.getProperty(ClosedEconomyWorkload.FIELD_NAME,
                                                  ClosedEconomyWorkload.DEFAULT_FIELD_NAME);

    standardQuery = new StringBuilder()
        .append("SELECT * FROM ").append(table).append(" WHERE id=@key").toString();
    standardScan = new StringBuilder()
        .append("SELECT * FROM ").append(table).append(" WHERE id>=@startKey LIMIT @count").toString();
    standardValidate = new StringBuilder().append("SELECT SUM(CAST(").append(validateField)
        .append(" AS INT64)) FROM ").append(validateTable).toString();
    for (int i = 0; i < fieldCount; i++) {
      STANDARD_FIELDS.add(fieldprefix + i);
    }
  }

  private static Spanner getSpanner(Properties properties, String host, String project) {
    if (spanner != null) {
      return spanner;
    }
    String numChannels = properties.getProperty(CloudSpannerProperties.NUM_CHANNELS);
    int numThreads = Integer.parseInt(properties.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
    SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder()
        .setSessionPoolOption(SessionPoolOptions.newBuilder()
            .setMinSessions(numThreads)
            .build());
    if (host != null) {
      optionsBuilder.setHost(host);
    }
    if (project != null) {
      optionsBuilder.setProjectId(project);
    }
    if (numChannels != null) {
      optionsBuilder.setNumChannels(Integer.parseInt(numChannels));
    }
    spanner = optionsBuilder.build().getService();
    Runtime.getRuntime().addShutdownHook(new Thread("spannerShutdown") {
        @Override
        public void run() {
          spanner.close();
        }
      });
    return spanner;
  }

  @Override
  public void init() throws DBException {
    synchronized (CLASS_LOCK) {
      if (dbClient != null) {
        return;
      }
      Properties properties = getProperties();
      String host = properties.getProperty(CloudSpannerProperties.HOST);
      String project = properties.getProperty(CloudSpannerProperties.PROJECT);
      String instance = properties.getProperty(CloudSpannerProperties.INSTANCE, "ycsb-instance");
      String database = properties.getProperty(CloudSpannerProperties.DATABASE, "ycsb-database");

      fieldCount = Integer.parseInt(properties.getProperty(
          CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
      queriesForReads = properties.getProperty(CloudSpannerProperties.READ_MODE, "query").equals("query");
      queriesForUpdates = properties.getProperty(CloudSpannerProperties.UPDATE_MODE, "update").equals("query");
      batchInserts = Integer.parseInt(properties.getProperty(CloudSpannerProperties.BATCH_INSERTS, "1"));
      constructStandardQueriesAndFields(properties);

      int boundedStalenessSeconds = Integer.parseInt(properties.getProperty(
          CloudSpannerProperties.BOUNDED_STALENESS, "0"));
      timestampBound = (boundedStalenessSeconds <= 0) ?
          TimestampBound.strong() : TimestampBound.ofMaxStaleness(boundedStalenessSeconds, TimeUnit.SECONDS);

      try {
        spanner = getSpanner(properties, host, project);
        if (project == null) {
          project = spanner.getOptions().getProjectId();
        }
        dbClient = spanner.getDatabaseClient(DatabaseId.of(project, instance, database));
      } catch (Exception e) {
        LOGGER.log(Level.SEVERE, "init()", e);
        throw new DBException(e);
      }

      LOGGER.log(Level.INFO, new StringBuilder()
          .append("\nHost: ").append(spanner.getOptions().getHost())
          .append("\nProject: ").append(project)
          .append("\nInstance: ").append(instance)
          .append("\nDatabase: ").append(database)
          .append("\nUsing queries for reads: ").append(queriesForReads)
          .append("\nBatching inserts: ").append(batchInserts)
          .append("\nBounded staleness seconds: ").append(boundedStalenessSeconds)
          .toString());
    }
  }

  // Read operations must be executed in the TransactionContext for transactional testing.
  private Status readUsingQuery(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Statement query;
    Iterable<String> columns = fields == null ? STANDARD_FIELDS : fields;
    if (fields == null || fields.size() == fieldCount) {
      query = Statement.newBuilder(standardQuery).bind("key").to(key).build();
    } else {
      Joiner joiner = Joiner.on(',');
      query = Statement.newBuilder("SELECT ")
          .append(joiner.join(fields))
          .append(" FROM ")
          .append(table)
          .append(" WHERE id=@key")
          .bind("key").to(key)
          .build();
    }

    try (ResultSet resultSet = tx.executeQuery(query)) {
      resultSet.next();
      decodeStruct(columns, resultSet, result);
      if (resultSet.next()) {
        LOGGER.log(Level.INFO, "readUsingQuery(): Expected exactly one row for each read.");
      }
      return Status.OK;
    } catch (AbortedException ae) {
      return Status.ERROR;
    } catch(Exception e) {
      LOGGER.log(Level.INFO, "readUsingQuery()", e);
      return Status.ERROR;
    }
  }

  // Read operations must be executed in the TransactionContext for transactional testing.
  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (queriesForReads) {
      return readUsingQuery(table, key, fields, result);
    }
    Iterable<String> columns = fields == null ? STANDARD_FIELDS : fields;
    try {
      Struct row = tx.readRow(table, Key.of(key), columns);
      decodeStruct(columns, row, result);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "read()", e);
      return Status.ERROR;
    }
  }

  // Scan operations must be executed in the TransactionContext for transactional testing.
  private Status scanUsingQuery(
      String table, String startKey, int recordCount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    Iterable<String> columns = fields == null ? STANDARD_FIELDS : fields;
    Statement query;
    if (fields == null || fields.size() == fieldCount) {
      query = Statement.newBuilder(standardScan).bind("startKey").to(startKey).bind("count").to(recordCount).build();
    } else {
      Joiner joiner = Joiner.on(',');
      query = Statement.newBuilder("SELECT ")
          .append(joiner.join(fields))
          .append(" FROM ")
          .append(table)
          .append(" WHERE id>=@startKey LIMIT @count")
          .bind("startKey").to(startKey)
          .bind("count").to(recordCount)
          .build();
    }

    try (ResultSet resultSet = tx.executeQuery(query)) {
      while (resultSet.next()) {
        HashMap<String, ByteIterator> row = new HashMap<>();
        decodeStruct(columns, resultSet, row);
        result.add(row);
      }
      return Status.OK;
    } catch (AbortedException ae) {
      return Status.ERROR;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "scanUsingQuery()", e);
      return Status.ERROR;
    }
  }

  // Scan operations must be executed in the TransactionContext for transactional testing.
  @Override
  public Status scan(
      String table, String startKey, int recordCount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    if (queriesForReads) {
      return scanUsingQuery(table, startKey, recordCount, fields, result);
    }
    Iterable<String> columns = fields == null ? STANDARD_FIELDS : fields;
    KeySet keySet =
        KeySet.newBuilder().addRange(KeyRange.closedClosed(Key.of(startKey), Key.of())).build();
    try (ResultSet resultSet = tx.read(table, keySet, columns, Options.limit(recordCount))) {
      while (resultSet.next()) {
        HashMap<String, ByteIterator> row = new HashMap<>();
        decodeStruct(columns, resultSet, row);
        result.add(row);
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "scan()", e);
      return Status.ERROR;
    }
  }

  // Update operations must be executed in the TransactionContext for transactional testing.
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (queriesForUpdates) {
      return updateUsingQuery(table, key, values);
    }
    Mutation.WriteBuilder m = Mutation.newInsertOrUpdateBuilder(table);
    m.set(PRIMARY_KEY_COLUMN).to(key);
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      m.set(e.getKey()).to(e.getValue().toString());
    }
    try {
      tx.buffer(Arrays.asList(m.build()));
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "update()", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (bufferedMutations.size() < batchInserts) {
      Mutation.WriteBuilder m = Mutation.newInsertOrUpdateBuilder(table);
      m.set(PRIMARY_KEY_COLUMN).to(key);
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        m.set(e.getKey()).to(e.getValue().toString());
      }
      bufferedMutations.add(m.build());
    } else {
      LOGGER.log(Level.INFO, "Limit of cached mutations reached. The given mutation with key " + key +
          " is ignored. Is this a retry?");
    }
    if (bufferedMutations.size() < batchInserts) {
      return Status.BATCHED_OK;
    }
    try {
      tx.buffer(bufferedMutations);
      bufferedMutations.clear();
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "insert()", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public void cleanup() {
    try {
      if (bufferedMutations.size() > 0) {
        transactionManager = dbClient.transactionManager();
        tx = transactionManager.begin();
        tx.buffer(bufferedMutations);
        transactionManager.commit();
        bufferedMutations.clear();
      }
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "cleanup()", e);
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      tx.buffer(Arrays.asList(Mutation.delete(table, Key.of(key))));
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "delete()", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  private static void decodeStruct(
      Iterable<String> columns, StructReader structReader, Map<String, ByteIterator> result) {
    for (String col : columns) {
      result.put(col, new StringByteIterator(structReader.getString(col)));
    }
  }

  @Override
  public void start() throws DBException {
    super.start();
    transactionManager = dbClient.transactionManager();
    tx = transactionManager.begin();
  }

  @Override
  public void commit() throws DBException {
    super.commit();
    try {
      transactionManager.commit();
    } catch (AbortedException e) {
      throw new DBException(e);
    }
  }

  @Override
  public void abort() throws DBException {
    super.abort();
    transactionManager.close();
  }

  @Override
  public long validate() throws DBException {
    super.validate();
    long countedSum;

    Statement query = Statement.newBuilder(standardValidate).build();

    try (ResultSet resultSet = dbClient.singleUse(timestampBound).executeQuery(query)) {
      resultSet.next();
      countedSum = resultSet.getLong(0);
      if (resultSet.next()) {
        throw new Exception("Expected exactly one row for validation.");
      }
      return countedSum;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "validate()", e);
      throw new DBException(e);
    }
  }

  private Status updateUsingQuery(String table, String key, Map<String, ByteIterator> values){
    if (values == null || values.isEmpty()) {
      LOGGER.log(Level.INFO, "updateUsingQuery(): Values are null.");
      return Status.ERROR;
    }

    HashSet<String> fields = new HashSet<>(values.keySet());
    Builder statementBuilder = Statement.newBuilder("UPDATE " + table + " SET ");
    // Iterate over the fields and append them to the query
    boolean firstField = true;
    for (String field : fields) {
      if (!firstField) {
        statementBuilder.append(", ");
      }
      statementBuilder.append(field).append(" = @").append(field);
      firstField = false;
    }
    // Append the WHERE clause
    statementBuilder.append(" WHERE id = @id");
    // Bind parameters
    Builder boundStatementBuilder = statementBuilder.bind("id").to(key);
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      boundStatementBuilder = boundStatementBuilder.bind(entry.getKey()).to(entry.getValue().toString());
    }
    Statement query = boundStatementBuilder.build();

      try {
        long rowCount = tx.executeUpdate(query);
        if (rowCount != 1) {
          throw new Exception("Expected to update exactly one row.");
        }
        return Status.OK;
      } catch (AbortedException ae) {
        return Status.ERROR;
      } catch(Exception e) {
        LOGGER.log(Level.INFO, "updateUsingQuery()", e);
        return Status.ERROR;
      }
  }

 }
