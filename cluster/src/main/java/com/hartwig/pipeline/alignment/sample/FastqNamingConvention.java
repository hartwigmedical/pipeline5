package com.hartwig.pipeline.alignment.sample;

import java.util.function.Predicate;
import java.util.regex.Pattern;

public class FastqNamingConvention implements Predicate<String> {

    @Override
    public boolean test(final String s) {
        return Pattern.matches("(.*_){2}S[0-9]+_L[0-9]{3}_R[1-2].*", s);
    }

    public static boolean apply(final String fastqFileName) {
        return new FastqNamingConvention().test(fastqFileName);
    }
}
