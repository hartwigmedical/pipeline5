package com.hartwig.pipeline.alignment.sample;

import java.util.function.Predicate;
import java.util.regex.Pattern;

public class FastqNamingConvention implements Predicate<String> {

    @Override
    public boolean test(final String s) {
        return Pattern.matches("(.*_){2}S[0-9]+_L[0-9]{3}_R[1-2]_[0-9]{3}.*", s);
    }

    public static void apply(final String fastqFileName) {
        if (!new FastqNamingConvention().test(fastqFileName)) {
            throw new IllegalArgumentException(String.format(
                    "Unable to extract flowcell and sample index from fastq name [%s]. Failing this run. Please rename fastq files to the "
                            + "correct convention {samplename}_{flowcell}_{sampleindex}_{laneindex}_{pairindex}_{suffix}.fastq.[gz].",
                    fastqFileName));
        }
    }
}
