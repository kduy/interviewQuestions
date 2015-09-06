package com.nventdata.task.storm.performance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Measure performance metrics into csv file
 */
public class Performance implements Serializable {

    public static Logger LOG = LoggerFactory.getLogger(Performance.class);

    /* name of application*/
    private String name;

    /*store metrics into Lists*/
    private List<Long> timestamps;
    private List<Long> recordsVolList;
    private List<Long> numOfRecordsList;

    /*temporary store metrics*/
    private long buffer ;
    private long counter;

    /*save metrics every 'interval' */
    private int interval;

    /* write stored metrics to file every 'dumpInterval'*/
    private long dumpInterval = 0;

    /*last moment at which metrics are written to file*/
    private long lastDump = 0;

    private String outputFolder;

    /* start measuring the performance*/
    private long startTime ;

    /*last moment at which metrics are stored to Lists*/
    protected  long lastAdd ;

    private boolean firstWrite;

    public Performance (String name , int interval , int dumpInterval, String outputFolder){
        timestamps = new ArrayList<Long>();
        recordsVolList = new ArrayList<Long>();
        numOfRecordsList = new ArrayList<Long>();
        this.name = name;
        this.outputFolder = outputFolder;
        this.interval = interval;
        this.dumpInterval = dumpInterval;
        counter = 0;
        buffer = 0;
        startTime = System.currentTimeMillis();
        firstWrite = true;
        lastAdd = System.currentTimeMillis();
    }

    /**
     * * Store measured data in 3 lists of timestamp, 
     *   number of records, and total volume of those records.
     *
     *   If time exceed the dumpInterval,
     *   print out all the list to file
     *
     * @param numOfRecords
     * @param recordsVolume
     * @param timestamp
     */
    public void add (long numOfRecords, long recordsVolume, long timestamp) {
        long ctime = timestamp - startTime;
        if (ctime < 0 )
            return;
        timestamps.add(ctime);
        recordsVolList.add(recordsVolume);
        numOfRecordsList.add(numOfRecords);

        // [optimal] write to file every 'dumpInterval'
        if (dumpInterval > 0) {
            if (ctime - lastDump > dumpInterval){
                writeCSV();
                lastDump = ctime;
                timestamps.clear();
                numOfRecordsList.clear();
                recordsVolList.clear();
            }
        } else {
            writeCSV();
        }
    }

    /**
     * * Tracking new records
     *
     * *  store metrics into Lists
     * * 
     * @param numOfNewRecords
     * @param newRecordsVolume
     */
    public void track (long numOfNewRecords, long newRecordsVolume){
        long ctime = System.currentTimeMillis();
        long dtime = ctime - lastAdd;
        buffer += newRecordsVolume;
        counter += numOfNewRecords;

        // add metrics to Lists every 'interval'
        if (dtime > interval){
            lastAdd = ctime - (ctime % interval);
            add(counter, buffer, ctime);
        }

    }

    private void writeCSV() {
        try {
            LOG.debug("Writing " + this.hashCode() + " thread:" +  Thread.currentThread().getId() + " CSV to output: " + outputFolder);
            FileWriter fw = new FileWriter(outputFolder+ "/"+ name + "_" + Thread.currentThread().getId() + ".csv", true);

            PrintWriter out = new PrintWriter(new BufferedWriter(fw));
            out.print(toString());

            out.close();
        } catch (FileNotFoundException e) {
            LOG.error("CSV output file not found: " + outputFolder);
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        StringBuilder csv = new StringBuilder();

        if (firstWrite) {
            firstWrite = false;
            csv.append("Time,#Records,Volume "+"\n");
        }

        for (int i = 0; i < timestamps.size(); i++) {
            csv.append(timestamps.get(i) + "," + numOfRecordsList.get(i) + "," + recordsVolList.get(i) + "\n");
        }

        return csv.toString();
    }

}
