package com.qlued.blobstore;

import java.util.List;

public class Test {

    public static void main(String[] args) {
        BlobStore store = new BlobStore();

        byte[] data = new byte[10];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        store.put("one/file.txt", data);
        store.put("two/file.txt", data);

        List<BlobMetadata> blobs = store.list();
        for (var blob : blobs) {
            System.out.println(blob.getName() + " " + blob.getCreationTime() + " " + blob.getSize() + " " + blob.isValid());
        }
    }
}
