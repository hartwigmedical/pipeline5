package com.hartwig.bcl2fastq.qc;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.hartwig.pipeline.jackson.ObjectMappers;

public class QualityControl {

    private final List<QualityCheck> checks;

    QualityControl(QualityCheck... checks) {
        this.checks = Arrays.asList(checks);
    }

    public QualityControlResults evaluate(String statsJson, String conversionLog) {
        try {
            Stats stats = ObjectMappers.get().readValue(statsJson, Stats.class);
            ImmutableQualityControlResults.Builder builder = QualityControlResults.builder();
            for (QualityCheck check : checks) {
                builder.addFlowcellLevel(check.apply(stats, conversionLog));
            }
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static QualityControl defaultQC() {
        return new QualityControl(new UnderminedReadPercentage(6), new ErrorsInLog());
    }
}
