package com.qlued.blobstore;

import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

public abstract class WriteOp implements AutoCloseable {

    private long startNanos = System.nanoTime();

    @Getter
    private long endNanos;

    private long txStartNanos;

    private long txSize;

    private int totalSize = 0;

    @Getter
    private boolean complete = false;

    @Getter
    private int txCount;

    @Getter
    @Setter
    private Throwable throwable;

    private transient MessageDigest digest;

    @Getter
    private byte[] hash;

    abstract protected byte[] readNextChunkInternal() throws IOException;

    byte[] readNextChunk() throws IOException {
        byte[] chunk = readNextChunkInternal();
        if (chunk == null) {
            complete = true;
            hash = digest.digest();
            return null;
        }

        txSize += chunk.length;
        totalSize += chunk.length;

        digest.update(chunk);

        return chunk;
    }

    protected WriteOp() {
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static WriteOp from(byte[] buf) {
        return new BytesWriteOp(buf);
    }

    static WriteOp from(InputStream inputStream) {
        return new InputStreamWriteOp(inputStream);
    }

    boolean continueInTx() {
        return !complete
                && (txSize + BlobStore.CHUNK_MAX_SIZE_BYTES < BlobStore.TX_SIZE_LIMIT_BYTES)
                && (System.nanoTime() - txStartNanos < BlobStore.TX_TIME_LIMIT_NANOS);
    }

    int currentOffset() {
        return totalSize;
    }

    int size() {
        return totalSize;
    }

    long getElapsedNanos() {
        return endNanos - startNanos;
    }

    int newTx() {
        txStartNanos = System.nanoTime();
        txSize = 0;
        return ++txCount;
    }

    @Override
    public void close() {
        endNanos = System.nanoTime();
    }
}
