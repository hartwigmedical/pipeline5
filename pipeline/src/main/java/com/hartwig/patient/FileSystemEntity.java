package com.hartwig.patient;

public interface FileSystemEntity {

    String directory();

    void accept(FileSystemVisitor visitor);
}
