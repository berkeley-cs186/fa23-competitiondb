package edu.berkeley.cs186.database.contest;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class Workload {

    // Various workload sizes
    public static final int TINY = 32;
    public static final int SMALL = 1000;
    public static final int MEDIUM = 3000;
    public static final int LARGE = 5000;

    public static final Iterable<String>  workloadTables = Arrays.asList("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier");
    public static final Iterable<String> workloadQueries =
            Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13");


    private final String id;

    private final int size;

    private final String queryFile;

    public Workload(String id, int size) {
        this.id = id;
        this.size = size;
        this.queryFile = id + ".sql";
    }

    public static String sizeToString(int size) {
        switch(size){
            case (Workload.TINY): return "Tiny";
            case (Workload.SMALL): return "Small";
            case (Workload.MEDIUM): return "Medium";
            case (Workload.LARGE): return "Large";
            default: return "Unknown size";
        }
    }
    public static Iterable<Workload> getWorkloadsBySize(int size) {
        ArrayList<Workload> workloads = new ArrayList<>();
        for (String id : Workload.workloadQueries) {
            workloads.add(new Workload(id, size));
        }
        return workloads;
    }

    public String getID() {
        return this.id;
    }

    public int getSize() {
        return this.size;
    }

    public String getQueryFile() {
        return this.queryFile;
    }

    public String getQuery() {
        InputStream inStream = Workload.class.getClassLoader().getResourceAsStream("contest/queries/" + this.getQueryFile());
        if (inStream == null)
            throw new RuntimeException("Could not ready query file: " + this.queryFile);

        StringBuilder query = new StringBuilder();
        String line;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inStream, StandardCharsets.UTF_8))){
            while ((line = reader.readLine()) != null) {
                query.append(" ").append(line.trim());
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to load Workload " + this.getQueryFile());
        }
        return query.toString();
    }

    public String toString() {
        return this.id + "_" + this.size;
    }

}
