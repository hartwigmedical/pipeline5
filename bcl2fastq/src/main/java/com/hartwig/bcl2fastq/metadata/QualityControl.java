package com.hartwig.bcl2fastq.metadata;

public class QualityControl {

    static boolean errorsInLogs(){
        return true;
    }

    static boolean undeterminedReadPercentage(){
        return true;
    }

    public static boolean minimumYield() {
        return true;
    }

    public static boolean minimumQ30() {
        return true;
    }
}
