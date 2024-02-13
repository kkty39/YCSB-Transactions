/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */

package site.ycsb.workloads;

import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import site.ycsb.*;
import site.ycsb.generator.ConstantIntegerGenerator;
import site.ycsb.generator.CounterGenerator;
import site.ycsb.generator.DiscreteGenerator;
import site.ycsb.generator.ExponentialGenerator;
import site.ycsb.generator.Generator;
import site.ycsb.generator.HistogramGenerator;
import site.ycsb.generator.HotspotIntegerGenerator;
import site.ycsb.generator.NumberGenerator;
import site.ycsb.generator.ScrambledZipfianGenerator;
import site.ycsb.generator.SkewedLatestGenerator;
import site.ycsb.generator.UniformLongGenerator;
import site.ycsb.generator.ZipfianGenerator;
import site.ycsb.measurements.Measurements;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD
 * operations. The relative proportion of different kinds of operations, and
 * other properties of the workload, are controlled by parameters specified at
 * runtime.
 *
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldCount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readAllFields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeAllFields</b>: should updates and read/modify/writes update all
 * fields (true) or just one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be
 * read a record, modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select
 * the records to operate on - uniform, zipfian, hotspot, or latest (default: uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be
 * used to choose the number of records to scan, for each scan, between 1 and
 * maxscanlength (default: uniform)
 * <LI><b>insertorder</b>: should records be inserted in order by key
 * ("ordered"), or in hashed order ("hashed") (default: hashed)
 * </ul>
 */
public class ClosedEconomyWorkload extends Workload {

  /**
   * The name of the database table to run queries against.
   */
  public static final String TABLE_NAME_PROPERTY = "table";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String TABLE_NAME_PROPERTY_DEFAULT = "usertable";
  /**
   * The name of the property for the number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY = "fieldCount";
  /**
   * Default number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";
  /**
   * The name of the property for the field length distribution. Options are
   * "uniform", "zipfian" (favoring short records), "constant", and
   * "histogram".
   *
   * If "uniform", "zipfian" or "constant", the maximum field length will be
   * that specified by the fieldlength property. If "histogram", then the
   * histogram will be read from the filename specified in the
   * "fieldlengthhistogram" property.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldLengthDistribution";
  /**
   * The default field length distribution.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";
  /**
   * The name of the property for the length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY = "fieldLength";
  /**
   * The default maximum length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";
  /**
   * The name of the property for the total amount of money in the economy at the start.
   */
  public static final String TOTAL_CASH_PROPERTY = "totalCash";
  /**
   * The default total amount of money in the economy at the start.
   */
  public static final String TOTAL_CASH_PROPERTY_DEFAULT = "1000000";
  public static final String OPERATION_COUNT_PROPERTY = "operationCount";
  /**
   * The name of a property that specifies the filename containing the field
   * length histogram (only used if fieldlengthdistribution is "histogram").
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldLengthHistogram";
  /**
   * The default filename containing a field length histogram.
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";
  /**
   * The name of the property for deciding whether to read one field (false)
   * or all fields (true) of a record.
   */
  public static final String READ_ALL_FIELDS_PROPERTY = "readAllFields";
  /**
   * The default value for the readAllFields property.
   */
  public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";
  /**
   * The name of the property for deciding whether to write one field (false)
   * or all fields (true) of a record.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY = "writeAllFields";
  /**
   * The default value for the writeAllFields property.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";
  /**
   * The name of the property for the proportion of transactions that are
   * reads.
   */
  public static final String READ_PROPORTION_PROPERTY = "readProportion";
  /**
   * The default proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";
  /**
   * The name of the property for the proportion of transactions that are
   * updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY = "updateProportion";
  /**
   * The default proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";
  /**
   * The name of the property for the proportion of transactions that are
   * inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY = "insertProportion";
  /**
   * The default proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";
  /**
   * The name of the property for the proportion of transactions that are
   * scans.
   */
  public static final String SCAN_PROPORTION_PROPERTY = "scanProportion";
  /**
   * The default proportion of transactions that are scans.
   */
  public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";
  /**
   * The name of the property for the proportion of transactions that are
   * read-modify-write.
   */
  public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readModifyWriteProportion";
  /**
   * The default proportion of transactions that are scans.
   */
  public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";
  /**
   * The name of the property for the the distribution of requests across the
   * keyspace. Options are "uniform", "zipfian" and "latest"
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
  /**
   * The value of theta used if a zipfian distribution is used to generate the requests.
   */
  public static final String ZIPFIAN_REQUEST_DISTRIBUTION_THETA = "zipfianrequestdistributiontheta";
  /**
   * The default value of theta used if a zipfian distribution is used to generate the requests.
   */
  public static final String ZIPFIAN_REQUEST_DISTRIBUTION_THETA_DEFAULT = "0.99";
  /**
   * The default distribution of requests across the keyspace
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";
  /**
   * The name of the property for the max scan length (number of records)
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
  /**
   * The default max scan length.
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";
  /**
   * The name of the property for the scan length distribution. Options are
   * "uniform" and "zipfian" (favoring short scans)
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";
  /**
   * The default max scan length.
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";
  /**
   * The name of the property for the order to insert records. Options are
   * "ordered" or "hashed"
   */
  public static final String INSERT_ORDER_PROPERTY = "insertorder";
  /**
   * Default insert order.
   */
  public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";
  /**
   * Percentage data items that constitute the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";
  /**
   * Default value of the size of the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";
  /**
   * Percentage operations that access the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";
  /**
   * Default value of the percentage operations accessing the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

  public static final String VALIDATE_BY_QUERY_PROPERTY = "validatebyquery";

  /**
   * The default value for the validateByQuery property.
   */
  public static final String VALIDATE_BY_QUERY_PROPERTY_DEFAULT = "true";

  public static String FIELD_NAME = "field";
  public static String DEFAULT_FIELD_NAME = "field0";

  public static String table;
  long fieldCount;
  /**
   * Generator object that produces field lengths. The value of this depends
   * on the properties that start with "FIELD_LENGTH_".
   */
  NumberGenerator fieldLengthGenerator;
  boolean readAllFields;
  boolean writeAllFields;
  NumberGenerator keySequence;
  NumberGenerator validationKeySequence;
  DiscreteGenerator operationChooser;
  NumberGenerator keyChooser;
  Generator fieldChooser;
  CounterGenerator transactionInsertKeySequence;
  NumberGenerator scanLength;
  boolean orderedInserts;
  long recordCount;
  long opCount;
  AtomicInteger actualOpCount = new AtomicInteger(0);
  private Measurements measurements;
  private final Hashtable<String, String> operations = new Hashtable<String, String>() {
    {
      put("READ", "TX-READ");
      put("UPDATE", "TX-UPDATE");
      put("INSERT", "TX-INSERT");
      put("SCAN", "TX-SCAN");
      put("READMODIFYWRITE", "TX-READMODIFYWRITE");
    }
  };
  private long totalCash;
  private long initialValue;
  private static long INITIAL_VALUE_DEFAULT = 1L;
  boolean validateByQuery;

  protected static NumberGenerator getFieldLengthGenerator(Properties p)
      throws WorkloadException {
    NumberGenerator fieldLengthGenerator;
    String fieldLengthDistribution = p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
        FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    int numRecords = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY));
    int totalCash = Integer.parseInt(
        p.getProperty(TOTAL_CASH_PROPERTY, TOTAL_CASH_PROPERTY_DEFAULT));

    long fieldLength = Long.parseLong(
        p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
    String fieldLengthHistogram = p.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY,
        FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
    if (fieldLengthDistribution.compareTo("constant") == 0) {
      fieldLengthGenerator = new ConstantIntegerGenerator(totalCash / numRecords);
    } else if (fieldLengthDistribution.compareTo("uniform") == 0) {
      fieldLengthGenerator = new UniformLongGenerator(1, totalCash / numRecords);
    } else if (fieldLengthDistribution.compareTo("zipfian") == 0) {
      fieldLengthGenerator = new ZipfianGenerator(1, fieldLength);
    } else if (fieldLengthDistribution.compareTo("histogram") == 0) {
      try {
        fieldLengthGenerator = new HistogramGenerator(fieldLengthHistogram);
      } catch (IOException e)
      {
        throw new WorkloadException(
            "Couldn't read field length histogram file: " + fieldLengthHistogram, e);
      }
    } else {
      throw new WorkloadException(
          "Unknown field length distribution \"" + fieldLengthDistribution + "\"");
    }
    return fieldLengthGenerator;
  }

  /**
   * Initialize the scenario. Called once, in the main client thread, before
   * any operations are started.
   */
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLE_NAME_PROPERTY, TABLE_NAME_PROPERTY_DEFAULT);

    fieldCount = Long.parseLong(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    fieldLengthGenerator = ClosedEconomyWorkload.getFieldLengthGenerator(p);

    double readProportion = Double.parseDouble(
        p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    double updateProportion = Double.parseDouble(
        p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
    double insertProportion = Double.parseDouble(
        p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    double scanProportion = Double.parseDouble(
        p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
    double readModifyWriteProportion = Double.parseDouble(
        p.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY,
            READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));

    opCount = Long.parseLong(p.getProperty(OPERATION_COUNT_PROPERTY, "0"));
    recordCount = Long.parseLong(p.getProperty(Client.RECORD_COUNT_PROPERTY));
    totalCash = Long.parseLong(p.getProperty(TOTAL_CASH_PROPERTY, TOTAL_CASH_PROPERTY_DEFAULT));
    long currentTotal = totalCash;
    long currentCount = recordCount;
    if (totalCash > 0 && totalCash % recordCount == 0) {
      initialValue = totalCash / recordCount;
    } else {
      System.err.println("Incompatible total cash value and record count. " +
          "Setting total cash value to record count, and cash values for each entry to 1.");
      totalCash = recordCount;
      initialValue = INITIAL_VALUE_DEFAULT;
    }

    String requestDistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,
        REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
    long maxScanLength = Long.parseLong(
        p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
    String scanLengthDistrib = p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
        SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    long insertStart = Long.parseLong(
        p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));

    readAllFields = Boolean.parseBoolean(
        p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
    writeAllFields = Boolean.parseBoolean(
        p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

    long insertCount=
        Integer.parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordCount - insertStart)));
    // Confirm valid values for insertstart and insertcount in relation to recordCount
    if (recordCount < (insertStart + insertCount)) {
      System.err.println("Invalid combination of insertstart, insertcount and recordCount.");
      System.err.println("recordCount must be bigger than insertstart + insertcount.");
      System.exit(-1);
    }

    if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed")
        == 0) {
      orderedInserts = false;
    } else if (requestDistrib.compareTo("exponential") == 0) {
      double percentile = Double.parseDouble(
          p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
              ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      double frac = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      keyChooser = new ExponentialGenerator(percentile, recordCount * frac);
    } else {
      orderedInserts = true;
    }

    keySequence = new CounterGenerator(insertStart);
    validationKeySequence = new CounterGenerator(insertStart);
    operationChooser = new DiscreteGenerator();
    if (readProportion > 0) {
      operationChooser.addValue(readProportion, "READ");
    }

    if (updateProportion > 0) {
      operationChooser.addValue(updateProportion, "UPDATE");
    }

    if (insertProportion > 0) {
      operationChooser.addValue(insertProportion, "INSERT");
    }

    if (readModifyWriteProportion > 0) {
      operationChooser.addValue(readModifyWriteProportion, "READMODIFYWRITE");
    }

    transactionInsertKeySequence = new CounterGenerator(recordCount);
    if (requestDistrib.compareTo("uniform") == 0) {
      keyChooser = new UniformLongGenerator(0, recordCount - 1);
    } else if (requestDistrib.compareTo("zipfian") == 0) {
      // it does this by generating a random "next key" in part by taking the modulus over the number of keys
      // if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
      // so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
      // of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
      // plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
      // just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled
      // zipfian generator
      long opCount = Long.parseLong(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
      long expectedNewKeys = (long) (((double) opCount) * insertProportion
          * 2.0); // 2 is fudge factor

      double theta = Double.parseDouble(p.getProperty(ZIPFIAN_REQUEST_DISTRIBUTION_THETA,
          ZIPFIAN_REQUEST_DISTRIBUTION_THETA_DEFAULT));

      keyChooser = new ScrambledZipfianGenerator(insertStart, insertStart + insertCount + expectedNewKeys, theta);
    } else if (requestDistrib.compareTo("latest") == 0) {
      keyChooser = new SkewedLatestGenerator(transactionInsertKeySequence);
    } else if (requestDistrib.equals("hotspot")) {
      double hotSetFraction = Double.parseDouble(
          p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotOpnFraction = Double.parseDouble(
          p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keyChooser = new HotspotIntegerGenerator(0, recordCount - 1, hotSetFraction, hotOpnFraction);
    } else {
      throw new WorkloadException("Unknown request distribution \"" + requestDistrib + "\"");
    }

    fieldChooser = new UniformLongGenerator(0, fieldCount - 1);

    if (scanLengthDistrib.compareTo("uniform") == 0) {
      scanLength = new UniformLongGenerator(1, maxScanLength);
    } else if (scanLengthDistrib.compareTo("zipfian") == 0) {
      scanLength = new ZipfianGenerator(1, maxScanLength);
    } else {
      throw new WorkloadException(
          "Distribution \"" + scanLengthDistrib + "\" not allowed for scan length");
    }

    measurements = Measurements.getMeasurements();

    validateByQuery = Boolean.parseBoolean(
        p.getProperty(VALIDATE_BY_QUERY_PROPERTY, VALIDATE_BY_QUERY_PROPERTY_DEFAULT));

    // write out config
    String s = "[CONFIG], READ_Proportion, " + readProportion + "\n" +
        "[CONFIG], UPDATE_Proportion, " + updateProportion + "\n" +
        "[CONFIG], INSERT_Proportion, " + insertProportion + "\n" +
        "[CONFIG], SCAN_Proportion, " + scanProportion + "\n" +
        "[CONFIG], READMODIFYWRITE_Proportion, " + readModifyWriteProportion + "\n" +
        "[CONFIG], Request_Distrib, " + requestDistrib + "\n" +
        "[CONFIG], Record_Count, " + recordCount;
    System.out.println(s);
  }

  public String buildKeyName(long keyNum) {
//		if (!orderedInserts) {
//			keyNum = Utils.hash(keyNum);
//		}
    // System.err.println("key: " + keyNum);
    return "user" + keyNum;
  }

  HashMap<String, ByteIterator> buildValues() {
    HashMap<String, ByteIterator> values = new HashMap<>();

    String fieldKey = DEFAULT_FIELD_NAME;
    ByteIterator data = new StringByteIterator("" + initialValue);
    values.put(fieldKey, data);
    return values;
  }

  HashMap<String, ByteIterator> buildUpdate() {
    // update a random field
    HashMap<String, ByteIterator> values = new HashMap<>();
    String fieldName = "field" + fieldChooser.nextString();
    ByteIterator data = new RandomByteIterator(
        fieldLengthGenerator.nextValue().longValue());
    values.put(fieldName, data);
    return values;
  }

  /**
   * Do one insert operation. Because it will be called concurrently from
   * multiple client threads, this function must be thread safe. However,
   * avoid synchronized, or the threads will block waiting for each other, and
   * it will be difficult to reach the target throughput. Ideally, this
   * function would have no side effects other than DB operations.
   *
   */
  public boolean doInsert(DB db, Object threadState) {
    long keyNum = keySequence.nextValue().longValue();
    String dbKey = buildKeyName(keyNum);
    HashMap<String, ByteIterator> values = buildValues();
    if (db.insert(table, dbKey, values).isOk()) {
      actualOpCount.addAndGet(1);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Do one insert operation. Because it will be called concurrently from
   * multiple client threads, this function must be thread safe. However,
   * avoid synchronized, or the threads will block waiting for each other, and
   * it will be difficult to reach the target throughput. Ideally, this
   * function would have no side effects other than DB operations.
   *
   */
  public boolean doDelete(DB db, Object threadState) {
    long keyNum = keySequence.nextValue().longValue();
    String dbKey = buildKeyName(keyNum);
    if (db.delete(table, dbKey).isOk()) {
      actualOpCount.addAndGet(1);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from
   * multiple client threads, this function must be thread safe. However,
   * avoid synchronized, or the threads will block waiting for each other, and
   * it will be difficult to reach the target throughput. Ideally, this
   * function would have no side effects other than DB operations.
   *
   */
  public boolean doTransaction(DB db, Object threadState) {
    boolean ret;
    long st = System.nanoTime();

    String op = operationChooser.nextString();

    if (op.equals("READ")) {
      ret = doTransactionRead(db);
    } else if (op.equals("UPDATE")) {
      ret = doTransactionUpdate(db);
    } else if (op.equals("INSERT")) {
      ret = doTransactionInsert(db);
    } else if (op.equals("SCAN")) {
      ret = doTransactionScan(db);
    } else {
      ret = doTransactionReadModifyWrite(db);
    }

    long en = System.nanoTime();
    measurements.measure(operations.get(op), (int) ((en - st) / 1000));
    if (ret) {
      measurements.reportStatus(operations.get(op), Status.OK);
    } else {
      measurements.reportStatus(operations.get(op), Status.ERROR);
    }
    actualOpCount.addAndGet(1);

    return ret;
  }

  long nextKeyNum() {
    long keyNum;
    if (keyChooser instanceof ExponentialGenerator) {
      do {
        keyNum = transactionInsertKeySequence.lastValue()
            - keyChooser.nextValue().longValue();
      } while (keyNum < 0);
    } else {
      do {
        keyNum = keyChooser.nextValue().longValue();
      } while (keyNum > transactionInsertKeySequence.lastValue());
    }
    return keyNum;
  }

  public boolean doTransactionRead(DB db) {
    // choose a random key
    long keyNum = nextKeyNum();

    String keyname = buildKeyName(keyNum);

    HashSet<String> fields = null;

    if (!readAllFields) {
      // read a random field
      String fieldName = "field" + fieldChooser.nextString();

      fields = new HashSet<>();
      fields.add(fieldName);
    }

    HashMap<String, ByteIterator> firstValues = new HashMap<>();

    return (db.read(table, keyname, fields, firstValues).isOk());
  }

  public boolean doTransactionReadModifyWrite(DB db) {
    // choose a random key
    long first = nextKeyNum();
    long second = first;
    while (second == first) {
      second = nextKeyNum();
    }
    if (first < second) {
      long temp = first;
      first = second;
      second = temp;
    }

    String firstKey = buildKeyName(first);
    String secondKey = buildKeyName(second);

    HashSet<String> fields = new HashSet<>();

    if (!readAllFields) {
      // read a random field
      String fieldName = "field" + fieldChooser.nextString();

      fields.add(fieldName);
    } else {
      fields.add(DEFAULT_FIELD_NAME);
    }

    HashMap<String, ByteIterator> firstValues = buildValues();
    HashMap<String, ByteIterator> secondValues = buildValues();

    // do the transaction
    long st = System.nanoTime();
    // For some SQL database, remember to enable "SELECT * FOR UPDATE" to lock the row during transaction
    if (db.read(table, firstKey, fields, firstValues).isOk() && db.read(table, secondKey, fields, secondValues).isOk()) {
      try {
        long firstamount = Long.parseLong(firstValues.get(DEFAULT_FIELD_NAME)
            .toString());
        long secondamount = Long.parseLong(secondValues.get(DEFAULT_FIELD_NAME)
            .toString());

        if (firstamount > 0) {
          firstamount--;
          secondamount++;
        }

        firstValues.put(DEFAULT_FIELD_NAME,
            new StringByteIterator(Long.toString(firstamount)));
        secondValues.put(DEFAULT_FIELD_NAME,
            new StringByteIterator(Long.toString(secondamount)));

        if (!db.update(table, firstKey, firstValues).isOk() ||
            !db.update(table, secondKey, secondValues).isOk()) return false;

        long en = System.nanoTime();

        Measurements.getMeasurements().measure("READ-MODIFY-WRITE",
            (int) (en - st) / 1000);
      } catch (NumberFormatException e) {
        return false;
      }
      return true;
    }
    return false;
  }

  public boolean doTransactionScan(DB db) {
    // choose a random key
    long keyNum = nextKeyNum();

    String startKeyName = buildKeyName(keyNum);

    // choose a random scan length
    int len = scanLength.nextValue().intValue();

    HashSet<String> fields = null;

    if (!readAllFields) {
      // read a random field
      String fieldName = "field" + fieldChooser.nextString();

      fields = new HashSet<>();
      fields.add(fieldName);
    }

    return (db.scan(table, startKeyName, len, fields,
        new Vector<>()).isOk());
  }

  public boolean doTransactionUpdate(DB db) {
    // choose a random key
    long keyNum = nextKeyNum();

    String keyName = buildKeyName(keyNum);

    HashMap<String, ByteIterator> values;

    if (writeAllFields) {
      // new data for all the fields
      values = buildValues();
    } else {
      // update a random field
      values = buildUpdate();
    }

    return (db.update(table, keyName, values).isOk());
  }

  public boolean doTransactionInsert(DB db) {
    // choose the next key
    long keyNum = transactionInsertKeySequence.nextValue();

    String dbKey = buildKeyName(keyNum);

    HashMap<String, ByteIterator> values = buildValues();
    return (db.insert(table, dbKey, values).isOk());
  }

  private long validateByRead(DB db) throws DBException {
    HashSet<String> fields = new HashSet<>();
    fields.add(DEFAULT_FIELD_NAME);
    HashMap<String, ByteIterator> values = new HashMap<>();
    long counted_sum = 0;
    long st = System.nanoTime();
    for (long i = 0; i < recordCount; i++) {
      String keyname = buildKeyName(validationKeySequence.nextValue().longValue());

      db.start();
      db.read(table, keyname, fields, values);
      db.commit();

      counted_sum += Long.parseLong(values.get(DEFAULT_FIELD_NAME).toString());
    }
    long en = System.nanoTime();
    // using Measurement class would exceed the MAX_VALUE of Integer,
    // so we directly print out the latency
    System.out.println("[VALIDATE], AverageLatency(us), " + (en - st) / 1000);
    return counted_sum;
  }

  /**
   * Perform validation of the database db after the workload has executed.
   *
   * @return false if the workload left the database in an inconsistent state, true if it is consistent.
   * @throws WorkloadException
   */
  public boolean validate(DB db) throws WorkloadException {
    long counted_sum;
    System.out.println("Validating data...");
    try {
      if (validateByQuery) {
        System.err.println("Validating by query execution...");
        counted_sum = db.validate();
        System.out.println("[VALIDATE], METHOD, QUERY");
      } else {
        System.err.println("Validating by read operations...");
        counted_sum = validateByRead(db);
        System.out.println("[VALIDATE], METHOD, READ-OPERATIONS");
      }
    } catch(Exception e) {
      throw new WorkloadException(e);
    }

    if (counted_sum == -1) {
      System.err.println("No validation done due to no validate() implementation in this database client.");
      System.out.println("[VALIDATE], STATUS, FAILED-NO IMPLEMENTATION");
      return false;
    }

    long count = actualOpCount.intValue();
    double anomalyScore = Math.abs((totalCash - counted_sum) / (1.0 * count));

    if (counted_sum != totalCash) {
      // validation failed, to terminal and to file
      printValidationMessages(System.err, "FAILED", totalCash, counted_sum, count, anomalyScore);
      printValidationMessages(System.out, "FAILED", totalCash, counted_sum, count, anomalyScore);
      return false;
    } else {
      // validation succeeded
      printValidationMessages(System.out, "SUCCESS", totalCash, counted_sum, count, anomalyScore);
      return true;
    }
  }

  // print validation messages
  private static void printValidationMessages(PrintStream stream, String status, long totalCash,
                                              long counted_sum, long count, double anomalyScore) {
    stream.println("[VALIDATE], STATUS, " + status);
    stream.println("[VALIDATE], TOTAL CASH, " + totalCash);
    stream.println("[VALIDATE], COUNTED CASH, " + counted_sum);
    stream.println("[VALIDATE], ACTUAL OPERATIONS, " + count);
    stream.println("[VALIDATE], ANOMALY SCORE, " + anomalyScore);
  }
}
