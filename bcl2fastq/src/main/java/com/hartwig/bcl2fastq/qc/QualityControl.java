package com.hartwig.bcl2fastq.qc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.hartwig.pipeline.jackson.ObjectMappers;

public class QualityControl {

    private final List<FlowcellQualityCheck> flowcellChecks;
    private final List<SampleQualityCheck> sampleChecks;

    QualityControl(final List<SampleQualityCheck> sampleChecks, FlowcellQualityCheck... flowcellChecks) {
        this.sampleChecks = sampleChecks;
        this.flowcellChecks = Arrays.asList(flowcellChecks);
    }

    public QualityControlResults evaluate(String statsJson, String conversionLog) {
        try {
            Stats stats = ObjectMappers.get().readValue(statsJson, Stats.class);
            List<QualityControlResult> flowcellLevel = new ArrayList<>();
            Multimap<String, QualityControlResult> sampleLevel = ArrayListMultimap.create();

            for (FlowcellQualityCheck check : flowcellChecks) {
                flowcellLevel.add(check.apply(stats, conversionLog));
            }
            for (SampleQualityCheck sampleCheck : sampleChecks) {
                sampleCheck.apply(stats).forEach(sampleLevel::put);
            }
            return QualityControlResults.builder().addAllFlowcellLevel(flowcellLevel).putAllSampleLevel(sampleLevel.asMap()).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static QualityControl defaultQC() {
        return new QualityControl(ImmutableList.of(new SampleMinimumYield(1_000_000_000)),
                new UnderminedReadPercentage(6),
                new ErrorsInLog());
    }
}
