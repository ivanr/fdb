package com.qlued.fdb.filestore;

import java.util.List;

public class Test {

    public static void main(String[] args) {
        FileStore fs = new FileStore();

        byte[] data = new byte[10];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        fs.put("file.txt", data);

        List<FileMetadata> files = fs.list();
        for (var file : files) {
            System.out.println(file.getName() + " " + file.getCreationTime() + " " + file.getSize() + " " + file.isValid());
        }
    }
}
