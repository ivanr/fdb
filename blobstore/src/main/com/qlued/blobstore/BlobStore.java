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
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

@Slf4j
public class BlobStore {

    private static final String DATA_PREFIX = "blobstore/data/";

    private static final String METADATA_PREFIX = "blobstore/metadata/";

    private static final byte[] TUPLE_DELIMITER = new byte[]{0x02};

    // FoundationDB has a limit of 10KB per key value.
    static final int CHUNK_MAX_SIZE_BYTES = 10_000;

    private static final long NANOS_PER_MILLISECOND = 1_000_000L;

    // FoundationDB limits transactions to 5 seconds.
    static final long TX_TIME_LIMIT_NANOS = 4_000 * NANOS_PER_MILLISECOND;

    // FoundationDB limits affected data within a transaction to 10 MB. I've
    // seen smaller sizes recommended (e.g., 1MB), but in my limited testing,
    // with a single blob, that results in 3x worse throughput.
    public static final int TX_SIZE_LIMIT_BYTES = 9_000_000;

    private FDB fdb;

    private static final Kryo kryo;

    static {
        kryo = new Kryo();
        kryo.register(BlobMetadata.class);
        kryo.register(byte[].class);
        kryo.register(Instant.class);
    }

    public BlobStore() {
        fdb = FDB.selectAPIVersion(730);
    }

    public void put(String key, byte[] value) {
        WriteOp op = WriteOp.from(value);
        put(key, op);
    }

    public void put(String key, InputStream inputStream) {
        WriteOp op = WriteOp.from(inputStream);
        put(key, op);
    }

    public void put(String key, WriteOp op) {

        byte[] metaKey = Tuple.from(METADATA_PREFIX, key).pack();
        BlobMetadata meta = new BlobMetadata();
        meta.setChunkSize(CHUNK_MAX_SIZE_BYTES);

        try (Database db = fdb.open()) {

            // Create the initial metadata entry, marking the file as invalid.
            op.newTx();
            db.run(tr -> {
                tr.set(metaKey, serialize(meta));
                return null;
            });

            // Upload the data in chunks, using multiple transactions if necessary.

            while (!op.isComplete()) {
                // Write multiple chunks in the same transaction
                // until we write all the data or run out of time.
                op.newTx();
                db.run(tr -> {
                    do {
                        try {
                            int offset = op.currentOffset();
                            byte[] chunk = op.readNextChunk();
                            if (chunk == null) {
                                // No more data.
                                return null;
                            }

                            Tuple chunkKey = Tuple.from(DATA_PREFIX, key, offset);
                            // System.err.println(chunkKey);
                            tr.set(chunkKey.pack(), chunk);
                        } catch (IOException e) {
                            // Failed reading from input.
                            tr.cancel();
                            op.setThrowable(e);
                            return null;
                        }
                    } while (op.continueInTx());
                    return null;
                });
            }

            // Complete the write operation by updating the metadata.

            meta.setValid(true);
            meta.setSize(op.getTotalSize());
            meta.setChunks(op.getTotalChunks());
            meta.setCreationTime(Instant.now());
            meta.setHash(op.getHash());

            op.newTx();
            db.run(tr -> {
                tr.set(metaKey, serialize(meta));
                return null;
            });

            op.close();
            log.info("Wrote " + op.getTotalSize() + " bytes in " + op.getTxCount()
                    + " transactions and " + (op.getElapsedNanos() / NANOS_PER_MILLISECOND) + " ms");
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

    public void get(String key) throws Exception {
        long startNanos = System.nanoTime();
        MessageDigest digest = MessageDigest.getInstance("SHA-256");

        Tuple start = Tuple.from(DATA_PREFIX, key);
        try (Database db = fdb.open()) {
            db.run(tr -> {
                for (KeyValue kv : tr.snapshot().getRange(start.range())) {
                    digest.update(kv.getValue());
                }
                return null;
            });
        }

        byte[] hash = digest.digest();
        System.err.println("Hash: " + HexFormat.of().formatHex(hash));
        System.err.println("Duration: " + ((System.nanoTime() - startNanos) / NANOS_PER_MILLISECOND) + " ms");
    }
}
