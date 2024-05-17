package com.qlued.blobstore;

import java.util.Arrays;

public class WriteOperation {

    private byte[] data;

    private long startNanos = System.nanoTime();

    private int offset = 0;

    private int left;

    WriteOperation(byte[] value) {
        this.data = value;
        this.left = value.length;
    }

    public byte[] chunk() {
        int len = BlobStore.CHUNK_MAX_SIZE <= left ? BlobStore.CHUNK_MAX_SIZE : left;
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
        return (left > 0) && (System.nanoTime() - startNanos < BlobStore.TX_TIME_LIMIT_NANOS);
    }

    int offset() {
        return offset;
    }

    int size() {
        return offset;
    }
}
