package edu.berkeley.cs186.database.contest;

import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.table.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestContest {
    // Final IO costs
    private Map<Workload, Long> ioCounts;

    // Student implementation
    private ContestRookieDB rookieDB;

    // Baseline implementation to verify answers
    private ContestBaselineDB baselineDB;

    @Before
    public void setup() {
        this.rookieDB = new ContestRookieDB(new LockManager(), ContestSetup.EVICTION_POLICY);
        this.baselineDB = new ContestBaselineDB();
        this.ioCounts = new HashMap<>();
    }

    @After
    public void cleanup() {
        this.rookieDB.close();
        this.baselineDB.close();
    }

    public void setupDatabases(int size) {
        this.rookieDB.loadWorkloadTables(size);
        this.rookieDB.endCurrentTransaction();
        this.ioCounts.put(new Workload("index", size), this.rookieDB.buildIndices(ContestSetup.INDICES_TO_BUILD));
        this.rookieDB.endCurrentTransaction();
        this.baselineDB.loadWorkloadTables(size);
    }

    private void verifyResults(Iterator<Record> studentRecords, Iterator<Record> expectedRecords) {
        Record studentRecord;
        Record expectedRecord;
        while (studentRecords.hasNext() && expectedRecords.hasNext()) {
            studentRecord = studentRecords.next();
            expectedRecord = expectedRecords.next();
            assertEquals(studentRecord, expectedRecord);
        }
        assertFalse(studentRecords.hasNext());
        assertFalse(expectedRecords.hasNext());
    }

    private long runWorkload(Workload workload) {
        long startingIOCount = this.rookieDB.getCurrentIOCount();
        Iterator<Record> studentResult = this.rookieDB.runWorkload(workload);
        Iterator<Record> baselineResult = this.baselineDB.runWorkload(workload);
        this.verifyResults(studentResult, baselineResult);
        return this.rookieDB.getCurrentIOCount() - startingIOCount;
    }

    public long runContest(int size) {
        Iterable<Workload> workloads = Workload.getWorkloadsBySize(size);
        this.setupDatabases(size);
        long ioCount;
        long totalIOCount = 0;
        for (Workload workload : workloads) {
            ioCount = runWorkload(workload);
            totalIOCount += ioCount;
            this.ioCounts.put(workload, ioCount);
        }
        return totalIOCount;
    }

    @Test
    public void runTinyContest() {
        runContest(Workload.TINY);
    }

    @Test
    public void runSmallContest(){
        runContest(Workload.SMALL);
    }

    @Test
    public void runMediumContest() {
        runContest(Workload.MEDIUM);
    }

    @Test
    public void runLargeContest() {
        runContest(Workload.LARGE);
    }


}
