package com.qlued.blobstore;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class InputStreamWriteOp extends WriteOp {

    private InputStream inputStream;

    private byte[] chunk = new byte[BlobStore.CHUNK_MAX_SIZE_BYTES];

    InputStreamWriteOp(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    protected byte[] readNextChunkInternal() throws IOException {

        int bytesRead = inputStream.read(chunk);
        if (bytesRead == -1) {
            return null;
        }

        if (bytesRead < BlobStore.CHUNK_MAX_SIZE_BYTES) {
            return Arrays.copyOfRange(chunk, 0, bytesRead);
        } else {
            return chunk;
        }
    }

    @Override
    public void close() {
        super.close();

        try {
            inputStream.close();
        } catch (IOException e) {
            // Intentionally left blank.
        }
    }
}
