package com.hartwig.pipeline.io;

public interface CloudCopy {

    void copy(String copyId, String from, String to, String... metadata);
}
