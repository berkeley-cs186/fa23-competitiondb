package edu.berkeley.cs186.database.contest;

import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ContestRunner {

    // Constants
    public static final int PRINT_N_ROWS = 0;
    public static final boolean EXPORT_ROWS = true;
    public static final int WORKLOAD_SIZE_TO_RUN = Workload.TINY;
    public static final String EXPORT_PATH = "project/serialized_contest_results/";
    // Store the contest db to perform cleanup
    public static ContestRookieDB db;


    public static void runContest(int workloadSize, String outPath) throws IOException {
        System.out.println("Running workload size: " + Workload.sizeToString(workloadSize));
        db = new ContestRookieDB(new LockManager(), ContestSetup.EVICTION_POLICY);
        db.loadWorkloadTables(workloadSize);
        db.endCurrentTransaction();
        System.out.println("Done loading tables. Building indices...");
        long indexIOCost = db.buildIndices(ContestSetup.INDICES_TO_BUILD);
        System.out.println("Done building indices.");
        Iterator<Record> records;
        QueryOperator qp;
        File outfile;
        FileOutputStream outputStream;
        FileOutputStream schemaOutputStream;
        Record r;
        Schema schema;
        PrettyPrinter prettyPrinter = new PrettyPrinter();
        List<Record> recordsToPrint = new ArrayList<>();

        // Make the serialized records directory if needed
        if (EXPORT_ROWS) {
            String parent = "";
            String[] serializedDirSplit = EXPORT_PATH.split("/");
            String folderName = serializedDirSplit[serializedDirSplit.length - 1];
            if (serializedDirSplit.length > 1)
                parent = String.join("/", Arrays.copyOfRange(serializedDirSplit, 0, serializedDirSplit.length - 1));

            File serializedDir = new File(parent, folderName);
            if (!serializedDir.exists() && !serializedDir.mkdirs())
                throw new IOException("Could not make the serialized results directory!: " + serializedDir);
        }

        long totalIOCount = 0;
        for (Workload workload : Workload.getWorkloadsBySize(workloadSize)) {
            long currentIOCount = db.getCurrentIOCount();
            qp = db.execute(workload.getQuery(), true);
            records = qp.iterator();
            schema = qp.getSchema();
            outfile = new File(outPath , workload + "_result.bytes");
            outputStream = new FileOutputStream(outfile);
            int recordCount = -1;
            if (EXPORT_ROWS) {
                schemaOutputStream = new FileOutputStream(new File(outPath, workload + "_schema.bytes"));
                schemaOutputStream.write(schema.toBytes());
                schemaOutputStream.flush();
                schemaOutputStream.close();
            }
            while (records.hasNext()) {
                recordCount += 1;
                r = records.next();
                if (recordCount < PRINT_N_ROWS)
                    recordsToPrint.add(r);
                if (EXPORT_ROWS)
                    outputStream.write(r.toBytes(schema));
            }
            outputStream.flush();
            outputStream.close();
            db.endCurrentTransaction();

            if (recordsToPrint.size() > 0)
                prettyPrinter.printRecords(schema.getFieldNames(), recordsToPrint.iterator());
            recordsToPrint.clear();
            totalIOCount += db.getCurrentIOCount() - currentIOCount;
            System.out.println("Query " + workload.getID() + " IO Total: " + (db.getCurrentIOCount() - currentIOCount));
        }
        db.close();
        System.out.println("Done running workload for size: " + Workload.sizeToString(workloadSize));
        System.out.println("Cost to build indices: " + indexIOCost);
        System.out.println("IO Cost for queries: " + totalIOCount);
        System.out.println("Final Workload IO Count: " + (totalIOCount + indexIOCost));
    }


    public static void main(String[] args) {
        try {
            runContest(WORKLOAD_SIZE_TO_RUN, EXPORT_PATH);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.out.println("Ran into a problem writing contest results! This is most likely an I/O issue.");
        } finally {
            if (db != null)
                db.close();
        }
    }
}
