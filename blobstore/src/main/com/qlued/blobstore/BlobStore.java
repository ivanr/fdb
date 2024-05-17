package com.qlued.blobstore;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class BlobStore {

    private static final String DATA_PREFIX = "blobstore/data/";

    private static final String METADATA_PREFIX = "blobstore/metadata/";

    private static final byte[] TUPLE_DELIMITER = new byte[]{0x02};

    static final int CHUNK_MAX_SIZE = 1;

    static final long TX_TIME_LIMIT_NANOS = 1_000;

    private FDB fdb;

    private static final Kryo kryo;

    static {
        kryo = new Kryo();
        kryo.register(BlobMetadata.class);
        kryo.register(Instant.class);
    }

    public BlobStore() {
        fdb = FDB.selectAPIVersion(730);
    }

    public void put(String key, byte[] value) {

        byte[] metaKey = Tuple.from(METADATA_PREFIX, key).pack();
        BlobMetadata meta = new BlobMetadata();

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

    private byte[] serialize(BlobMetadata meta) {
        try (Output output = new Output(new ByteArrayOutputStream())) {
            kryo.writeObject(output, meta);
            return output.toBytes();
        }
    }

    private BlobMetadata deserialize(byte[] data) {
        try (Input input = new Input(new ByteArrayInputStream(data))) {
            return kryo.readObject(input, BlobMetadata.class);
        }
    }

    public List<BlobMetadata> list() {
        return list("");
    }

    public List<BlobMetadata> list(@NonNull String relativePrefix) {
        List<BlobMetadata> files = new ArrayList<>();

        Tuple start = Tuple.from(METADATA_PREFIX);
        byte[] absolutePrefix = ByteArrayUtil.join(
                start.pack(),
                TUPLE_DELIMITER,
                relativePrefix.getBytes());

        try (Database db = fdb.open()) {
            db.run(tr -> {
                for (KeyValue kv : tr.getRange(Range.startsWith(absolutePrefix))) {
                    String name = Tuple.fromBytes(kv.getKey()).getString(1);
                    BlobMetadata meta = deserialize(kv.getValue());
                    meta.setName(name);
                    files.add(meta);
                }
                return null;
            });
        }

        return files;
    }
}
