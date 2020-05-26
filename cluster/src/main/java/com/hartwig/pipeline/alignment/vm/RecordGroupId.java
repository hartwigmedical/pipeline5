package com.hartwig.pipeline.alignment.vm;

import java.io.File;

public class RecordGroupId {

    static String from(String fastq) {
        return new File(fastq).getName().replace("_R1", "").replace("_R2", "").replace(".fastq", "").replace(".gz", "");
    }
}
