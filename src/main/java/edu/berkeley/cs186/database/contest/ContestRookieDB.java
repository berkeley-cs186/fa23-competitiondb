package edu.berkeley.cs186.database.contest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Iterator;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.memory.EvictionPolicy;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;


public class ContestRookieDB implements ContestDB, AutoCloseable {
    private static final int NUM_MEMORY_PAGES = 256;

    // Keep track of current query plans
    private final List<QueryPlan> queryPlans;

    // Current Transaction
    private Transaction currentTransaction = null;

    // Temporary database storage
    private Path dbPath;

    private final Database db;

    public ContestRookieDB(LockManager lockManager, EvictionPolicy policy) {
        try {
            this.dbPath = Files.createTempDirectory(Paths.get("./"), "rookieDBStorage");
            this.dbPath.toFile().deleteOnExit();
        } catch (IOException e) {
            System.err.println("Failed to generate a temporary directory for RookieDB!");
            e.printStackTrace();
            System.exit(1);
        }

        this.db = new Database(this.dbPath.toString(), NUM_MEMORY_PAGES, lockManager, policy);
        this.queryPlans = new ArrayList<>();
    }

    public long buildIndices(String[][] indicesToBuild) {
        long startingIOCount = this.getCurrentIOCount();
        String indexTable;
        String indexColumn;
        Transaction transaction = this.db.beginTransaction();
        for (String[] index : indicesToBuild) {
            indexTable = index[0];
            indexColumn = index[1];
            transaction.createIndex(indexTable, indexColumn, false); // Bulk load not supported right now
        }
        transaction.commit();
        db.waitAllTransactions();
        return this.getCurrentIOCount() - startingIOCount;
    }

    @Override
    public Iterator<Record> runWorkload(Workload workload) {
        String query = workload.getQuery();
        QueryOperator queryOperator = this.execute(query, true);
        return queryOperator.iterator();
    }

    @Override
    public boolean loadWorkloadTables(int size) {
        for (String tableName : Workload.workloadTables) {
            try {
                this.db.loadDelimitedFile("contest/tables/", tableName, "\\|", "_" + size + ".tbl");
            }
            catch (IOException e) {
                throw new RuntimeException("Could not load table: " + tableName + "_" + size + ".tbl");
            }
        }
        return true;
    }

    @Override
    public long getCurrentIOCount() {
        return this.db.getBufferManager().getNumIOs();
    }

    public QueryOperator execute(String statement, boolean useCurrentTransaction) throws DatabaseException {
        Transaction statementTransaction = useCurrentTransaction && this.currentTransaction != null ? this.currentTransaction : this.db.beginTransaction();
        Optional<QueryPlan> optionalQP = statementTransaction.execute(statement);

        if (optionalQP.isEmpty()) {
            throw new DatabaseException("Could not generate generate a query plan for statement: " + statement + "!");
        }
        QueryPlan queryPlan = optionalQP.get();
        this.queryPlans.add(queryPlan);
            this.currentTransaction = useCurrentTransaction && this.currentTransaction != null ? this.currentTransaction : statementTransaction;
        queryPlan.execute();
        return queryPlan.getFinalOperator();
    }

    public void endCurrentTransaction() {
        if (this.currentTransaction != null) {
            this.currentTransaction.commit();
        }
        this.db.waitAllTransactions();
        this.currentTransaction = null;
    }

    @Override
    public void close() {
        if (this.db != null) {
            this.endCurrentTransaction();
            this.db.close();
        }
        File dbFile = this.dbPath.toFile();
        if (dbFile.exists()) {
            File[] dbFiles = dbFile.listFiles();
            if (dbFiles != null) {
                for (File f : dbFiles)
                    f.delete();
            }
            dbFile.delete();
        }
    }

}
