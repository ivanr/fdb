package com.qlued.blobstore;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class InputStreamWriteOp extends WriteOp {

    private InputStream inputStream;

    private byte[] chunk = new byte[BlobStore.CHUNK_MAX_SIZE_BYTES];

    public InputStreamWriteOp(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    byte[] readNextChunk() throws IOException {

        int bytesRead = inputStream.read(chunk);
        if (bytesRead == -1) {
            complete = true;
            return null;
        }

        txSize += bytesRead;
        offset += bytesRead;

        if (bytesRead < BlobStore.CHUNK_MAX_SIZE_BYTES) {
            complete = true;
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
