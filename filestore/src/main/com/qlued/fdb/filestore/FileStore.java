package com.qlued.fdb.filestore;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.tuple.Tuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class FileStore {

    private static final String DATA_PREFIX = "filestore/data/";

    private static final String METADATA_PREFIX = "filestore/metadata/";

    static final int CHUNK_MAX_SIZE = 1;

    static final long TX_TIME_LIMIT_NANOS = 1_000;

    private FDB fdb;

    private static final Kryo kryo;

    static {
        kryo = new Kryo();
        kryo.register(FileMetadata.class);
        kryo.register(Instant.class);
    }

    public FileStore() {
        fdb = FDB.selectAPIVersion(730);
    }

    public void put(String key, byte[] value) {

        byte[] metaKey = Tuple.from(METADATA_PREFIX, key).pack();
        FileMetadata meta = new FileMetadata();

        try (Database db = fdb.open()) {

            // Create the initial metadata entry, marking the file as invalid.
            db.run(tr -> {
                tr.set(metaKey, serialize(meta));
                log.info("Write starting");
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
                                Tuple.from(DATA_PREFIX, key, offset).pack(),
                                Tuple.from(chunk).pack()
                        );
                        log.info("Write chunk offset " + offset + " len " + chunk.length);
                    } while (write.continueInTx());
                    return null;
                });
            }

            // Complete the write operation by updating the metadata.

            meta.setValid(true);
            meta.setSize(write.size());
            meta.setCreationTime(Instant.now());

            db.run(tr -> {
                tr.set(metaKey, serialize(meta));
                log.info("Write completed");
                return null;
            });
        }
    }

    private byte[] serialize(FileMetadata meta) {
        try (Output output = new Output(new ByteArrayOutputStream())) {
            kryo.writeObject(output, meta);
            return output.getBuffer();
        }
    }

    private FileMetadata deserialize(byte[] data) {
        try (Input input = new Input(new ByteArrayInputStream(data))) {
            return kryo.readObject(input, FileMetadata.class);
        }
    }

    public List<FileMetadata> list() {
        List<FileMetadata> files = new ArrayList<>();

        try (Database db = fdb.open()) {
            db.run(tr -> {
                for (KeyValue kv : tr.getRange(Tuple.from(METADATA_PREFIX).range())) {
                    String name = Tuple.fromBytes(kv.getKey()).getString(1);
                    FileMetadata meta = deserialize(kv.getValue());
                    meta.setName(name);
                    files.add(meta);
                }
                return null;
            });
        }

        return files;
    }
}
