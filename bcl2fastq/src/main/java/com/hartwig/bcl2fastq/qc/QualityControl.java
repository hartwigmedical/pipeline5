package com.hartwig.bcl2fastq.qc;

import java.io.IOException;
import java.io.InputStream;

import com.hartwig.bcl2fastq.ConvertedFastq;
import com.hartwig.pipeline.jackson.ObjectMappers;

public class QualityControl {

    QualityControlResult evaluate(ConvertedFastq fastq, InputStream statsJson) {
        try {
            Stats stats = ObjectMappers.get().readValue(statsJson, Stats.class);
            System.out.println(stats);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
