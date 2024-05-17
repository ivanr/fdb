package com.qlued.fdb.filestore;

import lombok.Data;

import java.time.Instant;

@Data
public class FileMetadata {

    private String name;

    private boolean valid;

    private Integer size;

    private Instant creationTime;
}
