package edu.berkeley.cs186.database.contest;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class ContestBaselineDB implements ContestDB{
    private static final String expectedDirectory = "contest/reference/";

    // Subclass to help read expected query results from disk
    private static class RecordDiskIterator implements Iterator<Record> {
        private final Schema schema;
        private final InputStream recordStream;
        private RecordDiskIterator(Schema schema, String recordsFile) {
            this.schema = schema;
            this.recordStream = ContestBaselineDB.class.getClassLoader().getResourceAsStream(expectedDirectory + recordsFile);
        }

        @Override
        public boolean hasNext() {
            boolean available;
            try {
                available = this.recordStream.available() > 0;
            }
            catch (IOException e) {
                available = false;
            }
            return available;
        }

        @Override
        public Record next() {
            Record r;
            try {
                Buffer recordBytes = ByteBuffer.wrap(this.recordStream.readNBytes(this.schema.getSizeInBytes()));
                r = Record.fromBytes(recordBytes, this.schema);
            } catch (IOException e) {
                r = null;
            }
            return r;
        }
    }

    @Override
    public boolean loadWorkloadTables(int size) {
        return true;
    }

    @Override
    public Iterator<Record> runWorkload(Workload workload) {
        Schema schema = null;
        try (InputStream schemaInstream = ContestBaselineDB.class.getClassLoader().getResourceAsStream(expectedDirectory + workload + "_schema.bytes")) {
            if (schemaInstream == null)
                throw new IOException("Failed to read bytes for schema bytes file: " + workload + "_schema.bytes");
            byte[] schemaBytes = schemaInstream.readAllBytes();
            schema = Schema.fromBytes(ByteBuffer.wrap(schemaBytes));
        } catch (IOException e) {
            System.err.println("Could not read the expected results for workload "  + workload);
        }
        return new RecordDiskIterator(schema, workload + "_result.bytes");
    }

    @Override
    public long getCurrentIOCount() {
        return 0;
    }

    @Override
    public void close() {}
}
