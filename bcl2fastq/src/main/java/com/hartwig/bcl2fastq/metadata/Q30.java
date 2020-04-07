package com.hartwig.bcl2fastq.metadata;

import com.hartwig.bcl2fastq.conversion.WithYieldAndQ30;

public class Q30 {

    public static double of(WithYieldAndQ30 withYieldAndQ30) {
        if (withYieldAndQ30.yieldQ30() > withYieldAndQ30.yield()) {
            throw new IllegalArgumentException(String.format(
                    "Tried to calculate a Q30 where the there was more Q30 yield than total yield. This must be "
                            + "an issue in the Stats.json which should be investigated. Yield was [%s] Q30 Yeild was [%s]",
                    withYieldAndQ30.yield(),
                    withYieldAndQ30.yieldQ30()));
        }
        if (withYieldAndQ30.yieldQ30() == 0){
            return 0d;
        }
        return withYieldAndQ30.yieldQ30() / (double) withYieldAndQ30.yield() * 100;
    }
}
