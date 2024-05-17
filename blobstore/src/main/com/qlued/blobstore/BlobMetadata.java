package com.qlued.blobstore;

import lombok.Data;

import java.time.Instant;

@Data
public class BlobMetadata {

    private String name;

    private boolean valid;

    private Integer size;

    private Instant creationTime;

    private byte[] hash;

}
