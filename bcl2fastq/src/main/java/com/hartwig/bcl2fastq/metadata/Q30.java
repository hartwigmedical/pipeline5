package com.hartwig.bcl2fastq.metadata;

import com.hartwig.bcl2fastq.conversion.WithYieldAndQ30;

public class Q30 {

    public static double of(WithYieldAndQ30 withYieldAndQ30) {
        return withYieldAndQ30.yieldQ30() / (double) withYieldAndQ30.yield() * 100;
    }
}
