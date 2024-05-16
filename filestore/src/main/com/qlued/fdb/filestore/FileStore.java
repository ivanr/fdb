package com.qlued.fdb.filestore;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
public class FileStore {

    private static final String DATA = "data";

    private static final String METADATA = "metadata";

    private static final String VALID = "valid";

    private static final int CHUNK_MAX_SIZE = 1;

    private static final long TX_TIME_LIMIT_NANOS = 1_000;

    private FDB fdb;

    private static class WriteOperation {

        private byte[] data;

        private long startNanos = System.nanoTime();

        private int offset = 0;

        private int left;

        WriteOperation(byte[] value) {
            this.data = value;
            this.left = value.length;
        }

        int offset() {
            return offset;
        }

        public byte[] chunk() {
            int len = CHUNK_MAX_SIZE <= left ? CHUNK_MAX_SIZE : left;
            try {
                return Arrays.copyOfRange(data, offset, offset + len);
            } finally {
                offset += len;
                left -= len;
            }
        }

        public boolean complete() {
            return left <= 0;
        }

        public boolean continueInTx() {
            return (left > 0) && (System.nanoTime() - startNanos < TX_TIME_LIMIT_NANOS);
        }
    }

    public FileStore() {
        fdb = FDB.selectAPIVersion(730);
    }

    public void put(String key, byte[] value) {

        try (Database db = fdb.open()) {

            // Create the initial metadata entry, marking the file as invalid.
            db.run(tr -> {
                tr.set(Tuple.from(key, METADATA, VALID).pack(), Tuple.from(false).pack());
                log.info("Marked file as invalid");
                return null;
            });

            // Upload the data in chunks, using multiple transactions if necessary.
            WriteOperation write = new WriteOperation(value);
            while (!write.complete()) {
                // Write multiple chunks in the same transaction
                // until we write all the data or run out of time.
                db.run(tr -> {
                    do {
                        int offset = write.offset();
                        byte[] chunk = write.chunk();
                        tr.set(
                                Tuple.from(key, DATA, offset).pack(),
                                Tuple.from(chunk).pack()
                        );
                        log.info("Write chunk offset " + offset + " len " + chunk.length);
                    } while (write.continueInTx());
                    return null;
                });
            }

            // Now mark the file as valid.
            db.run(tr -> {
                tr.set(Tuple.from(key, METADATA, VALID).pack(), Tuple.from(true).pack());
                log.info("Marked file as valid");
                return null;
            });
        }
    }

}
