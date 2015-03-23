/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gora.tutorial.log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import ais.raw.nmea.RawNMEA;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NmeaLogManager is modified from LogManager
 * 
 * LogManager is the tutorial class to illustrate the basic 
 * {@link DataStore} API usage. The LogManager class is used 
 * to parse the web server logs in combined log format, store the 
 * data in a Gora compatible data store, query and manipulate the stored data.  
 * 
 * <p>In the data model, keys are the line numbers in the log file, 
 * and the values are RawNMEA objects, generated from 
 * <code>gora-tutorial/src/main/avro/rawnmea.json</code>.
 * 
 * <p>See the tutorial.html file in docs or go to the 
 * <a href="http://gora.apache.org/docs/current/tutorial.html"> 
 * web site</a>for more information.</p>
 */
public class NmeaLogManager {

  private static final Logger log = LoggerFactory.getLogger(NmeaLogManager.class);
  
  private DataStore<Long, RawNMEA> dataStore; 
  
  private static final SimpleDateFormat dateFormat 
    = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
  
  public NmeaLogManager() {
    try {
      init();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
  
  private void init() throws IOException {
    //Data store objects are created from a factory. It is necessary to 
    //provide the key and value class. The datastore class is optional, 
    //and if not specified it will be read from the properties file
    dataStore = DataStoreFactory.getDataStore(Long.class, RawNMEA.class,
            new Configuration());
  }
  
  /**
   * Parses a log file and store the contents at the data store.
   * @param input the input file location
   */
  private void parse(String input) throws IOException, ParseException, Exception {
    log.info("Parsing file:" + input);
    BufferedReader reader = new BufferedReader(new FileReader(input));
    long lineCount = 0;
    try {
      String line = reader.readLine();
      do {
        RawNMEA rawnmea = parseLine(line);
        
        if(rawnmea != null) {
          //store the rawnmea 
          storeRawNMEA(lineCount++, rawnmea);
        }
        
        line = reader.readLine();
      } while(line != null);
      
    } finally {
      reader.close();  
    }
    log.info("finished parsing file. Total number of log lines:" + lineCount);
  }
  
  /** Parses a single log line in combined log format using StringTokenizers */
  private RawNMEA parseLine(String line) throws ParseException {
    String[] tokens = line.split(",|\\*");
    
    //construct and return rawnmea object
    RawNMEA rawnmea = new RawNMEA();
    if (tokens.length == 8){
        rawnmea.setPacketType(tokens[0]);
        rawnmea.setFragmentCount(tokens[1]);
        rawnmea.setFragmentNum(tokens[2]);
        rawnmea.setMessageId(tokens[3]);
        rawnmea.setChannelCode(tokens[4]);
        rawnmea.setDataPayload(tokens[5]);
        rawnmea.setPadding(tokens[6]);
        rawnmea.setChecksum(tokens[7]);
    }
    
    return rawnmea;
  }
  
  /** Stores the rawnmea object with the given key */
  private void storeRawNMEA(long key, RawNMEA rawnmea) throws IOException, Exception {
	log.info("Storing RawNMEA in: " + dataStore.toString());
	dataStore.put(key, rawnmea);
  }
  
  /** Fetches a single rawnmea object and prints it*/
  private void get(long key) throws IOException, Exception {
    RawNMEA rawnmea = dataStore.get(key);
    printRawNMEA(rawnmea);
  }
  
  /** Queries and prints a single rawnmea object */
  private void query(long key) throws IOException, Exception {
    //Queries are constructed from the data store
    Query<Long, RawNMEA> query = dataStore.newQuery();
    query.setKey(key);
    
    Result<Long, RawNMEA> result = query.execute(); //Actually executes the query.
    // alternatively dataStore.execute(query); can be used
    
    printResult(result);
  }
  
  /** Queries and prints rawnmea object that have keys between startKey and endKey*/
  private void query(long startKey, long endKey) throws IOException, Exception {
    Query<Long, RawNMEA> query = dataStore.newQuery();
    //set the properties of query
    query.setStartKey(startKey);
    query.setEndKey(endKey);
    
    Result<Long, RawNMEA> result = query.execute();
    
    printResult(result);
  }
  
  
  /**Deletes the rawnmea with the given line number */
  private void delete(long lineNum) throws Exception {
    dataStore.delete(lineNum);
    dataStore.flush(); //write changes may need to be flushed before
                       //they are committed 
    log.info("rawnmea with key:" + lineNum + " deleted");
  }
  
  /** This method illustrates delete by query call */
  private void deleteByQuery(long startKey, long endKey) throws IOException, Exception {
    //Constructs a query from the dataStore. The matching rows to this query will be deleted
    Query<Long, RawNMEA> query = dataStore.newQuery();
    //set the properties of query
    query.setStartKey(startKey);
    query.setEndKey(endKey);
    
    dataStore.deleteByQuery(query);
    log.info("rawnmeas with keys between " + startKey + " and " + endKey + " are deleted");
  }
  
  private void printResult(Result<Long, RawNMEA> result) throws IOException, Exception {
    
    while(result.next()) { //advances the Result object and breaks if at end
      long resultKey = result.getKey(); //obtain current key
      RawNMEA resultRawNMEA = result.get(); //obtain current value object
      
      //print the results
      printRawNMEA(resultRawNMEA);
    }
    
    System.out.println("Number of rawnmeas from the query:" + result.getOffset());
  }
  
  /** Pretty prints the rawnmea object to stdout */
  private void printRawNMEA(RawNMEA rawnmea) {
    if(rawnmea == null) {
      System.out.println("No result to show"); 
    } else {
      System.out.println(rawnmea.toString());
    }
  }
  
  private void close() throws IOException, Exception {
    //It is very important to close the datastore properly, otherwise
    //some data loss might occur.
    if(dataStore != null)
      dataStore.close();
  }
  
  private static final String USAGE = "NmeaLogManager -parse <input_log_file>\n" +
                                      "           -get <lineNum>\n" +
                                      "           -query <lineNum>\n" +
                                      "           -query <startLineNum> <endLineNum>\n" +
  		                                "           -delete <lineNum>\n" +
  		                                "           -deleteByQuery <startLineNum> <endLineNum>\n";
  
  public static void main(String[] args) throws Exception {
    if(args.length < 2) {
      System.err.println(USAGE);
      System.exit(1);
    }
    
    NmeaLogManager manager = new NmeaLogManager();
    
    if("-parse".equals(args[0])) {
      manager.parse(args[1]);
    } else if("-get".equals(args[0])) {
      manager.get(Long.parseLong(args[1]));
    } else if("-query".equals(args[0])) {
      if(args.length == 2) 
        manager.query(Long.parseLong(args[1]));
      else 
        manager.query(Long.parseLong(args[1]), Long.parseLong(args[2]));
    } else if("-delete".equals(args[0])) {
      manager.delete(Long.parseLong(args[1]));
    } else if("-deleteByQuery".equalsIgnoreCase(args[0])) {
      manager.deleteByQuery(Long.parseLong(args[1]), Long.parseLong(args[2]));
    } else {
      System.err.println(USAGE);
      System.exit(1);
    }
    
    manager.close();
  }
  
}
