package edu.berkeley.cs186.database.contest;

import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;


public interface ContestDB {

    boolean loadWorkloadTables(int size);

    Iterator<Record> runWorkload(Workload workload);

    long getCurrentIOCount();

    void close();


}
