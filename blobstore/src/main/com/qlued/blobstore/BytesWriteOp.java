package com.qlued.blobstore;

import java.util.Arrays;

public class BytesWriteOp extends WriteOp {

    private byte[] inputBuffer;

    private int offset;

    private int left;

    BytesWriteOp(byte[] buf) {
        this.inputBuffer = buf;
        this.left = buf.length;
    }

    @Override
    protected byte[] readNextChunkInternal() {
        int len = BlobStore.CHUNK_MAX_SIZE_BYTES <= left ? BlobStore.CHUNK_MAX_SIZE_BYTES : left;
        if (len == 0) {
            return null;
        }

        offset += len;
        left -= len;

        return Arrays.copyOfRange(inputBuffer, offset, offset + len);
    }
}
