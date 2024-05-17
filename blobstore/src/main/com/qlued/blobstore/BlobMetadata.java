package com.qlued.blobstore;

import lombok.Data;

import java.time.Instant;

@Data
public class BlobMetadata {

    private String name;

    private boolean valid;

    private Instant creationTime;

    private int size;

    private int chunks;

    private int chunkSize;

    private byte[] hash;

}
