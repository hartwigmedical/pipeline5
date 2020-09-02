package com.hartwig.pipeline.alignment.bwa;

import static org.apache.commons.io.FilenameUtils.getName;
import static org.apache.commons.io.FilenameUtils.removeExtension;

import java.util.regex.Pattern;

public interface RecordGroupId {

    static String from(boolean strict, String fastq) {
        String fastqNoExtension = removeFastqAndGz(getName(fastq));
        if (strict) {
            if (!Pattern.compile("^.*_.*_.*_L[0-9]{3}_R[1,2]_[0-9]{3}$").matcher(fastqNoExtension).matches()) {
                throw new IllegalArgumentException(String.format("Fastq file [%s] did not match the expected pattern of "
                        + "SAMPLENAME_FLOWCELLID_INDEX_LANE_PAIR_001.fastq.gz. Failing this run as this will cause issues later in the reads "
                        + "RG field", fastq));
            }
        }
        return fastqNoExtension.replace("_R1", "").replace("_R2", "");
    }

    static String removeFastqAndGz(final String name) {
        return removeExtension(removeExtension(name));
    }
}
