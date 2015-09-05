package com.nventdata.task.storm.performance;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kidio on 05/09/15.
 */
public class Performance implements Serializable {
    protected List<Long> timestamps;
    protected List<Long> recordsVolList;
    protected List<Long> numOfRecordsList;

    protected long dumpInterval = 0;
    protected long lastDump = 0;
    protected String fname;
    
    protected long startTime ;
    
    protected int interval;
    protected String name;
    
    protected long buffer ;
    protected long counter;
    protected boolean firstWrite;
    
    protected  long lastAdd ;

    public Performance(String name, int interval, int dumpInterval, String fname){
        timestamps = new ArrayList<Long>();
        recordsVolList = new ArrayList<Long>();
        numOfRecordsList = new ArrayList<Long>();
        this.name = name;
        this.fname = fname;
        this.interval = interval;
        this.dumpInterval = dumpInterval;
        counter = 0;
        buffer = 0;
        startTime = System.currentTimeMillis();
        firstWrite = true;
        lastAdd = System.currentTimeMillis();
    }

    /**
     * *   store measured data in list
     *   if time exceed the dumpInterval, 
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
    
    public void track (long numOfNewRecords, long newRecordsVolume){
        long ctime = System.currentTimeMillis();
        long dtime = ctime - lastAdd;
        buffer += newRecordsVolume;
        counter += numOfNewRecords;
        
        if (dtime > interval){
            lastAdd = ctime - (ctime % interval);
            add(counter, buffer, ctime);
        }
        
        
    }

    private void writeCSV() {
        try {
            System.out.println("Writing " + this.hashCode() + " thread:" +  Thread.currentThread().getId() + " CSV to output: " + fname);
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fname + "_" + Thread.currentThread().getId() + ".csv", true)));
            out.print(toString());
            out.close();
        } catch (FileNotFoundException e) {
            System.out.println("CSV output file not found: " + fname);

        } catch (IOException e) {
            e.printStackTrace();
        }
            
    }
    
    @Override
    public String toString() {
        StringBuilder csv = new StringBuilder();

        if (firstWrite) {
            firstWrite = false;
            csv.append("Time," + "#Records, Size," + ",Label\n");
        }

        for (int i = 0; i < timestamps.size(); i++) {
            csv.append(timestamps.get(i) + "," + numOfRecordsList.get(i) + "," + recordsVolList.get(i) + "\n");
        }

        return csv.toString();
    }



}
