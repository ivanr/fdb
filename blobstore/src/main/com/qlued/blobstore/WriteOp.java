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

    protected long txSize;

    protected int offset = 0;

    protected boolean complete = false;

    @Getter
    private int txCount;

    @Getter
    @Setter
    private Throwable throwable;

    private transient MessageDigest digest;

    private byte[] hash;

    abstract protected byte[] readNextChunkInternal() throws IOException;

    byte[] readNextChunk() throws IOException {
        byte[] chunk = readNextChunkInternal();
        if (chunk == null) {
            return null;
        }

        txSize += chunk.length;
        offset += chunk.length;

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
        return offset;
    }

    int size() {
        return offset;
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
        hash = digest.digest();
        endNanos = System.nanoTime();
    }

    boolean complete() {
        return complete;
    }
}
