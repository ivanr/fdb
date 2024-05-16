package com.qlued.fdb.filestore;

public class Test {

    public static void main(String[] args) {
        FileStore fs = new FileStore();

        byte[] data = new byte[10];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)i;
        }

        fs.put("file.txt", data);
    }
}
