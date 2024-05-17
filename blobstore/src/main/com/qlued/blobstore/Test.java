package com.qlued.blobstore;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class Test {

    public static void main(String[] args) throws IOException {
        BlobStore store = new BlobStore();

        byte[] data = new byte[10];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ('a' + i);
        }

        store.put("one/file.txt", data);

        //store.put("two/file.txt", new FileInputStream("blobstore/NOTES.txt"));
        store.put("two/file.txt", new FileInputStream("blobstore/largefile.dat"));

        List<BlobMetadata> blobs = store.list();
        for (var blob : blobs) {
            System.out.println(blob.getName() + " " + blob.getCreationTime() + " " + blob.getSize() + " " + blob.isValid());
        }
    }
}
