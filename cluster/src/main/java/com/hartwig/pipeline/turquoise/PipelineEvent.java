package com.hartwig.pipeline.turquoise;

import static com.hartwig.pipeline.turquoise.PipelineSubjects.BARCODE;
import static com.hartwig.pipeline.turquoise.PipelineSubjects.RUN_ID;
import static com.hartwig.pipeline.turquoise.PipelineSubjects.SAMPLE;
import static com.hartwig.pipeline.turquoise.PipelineSubjects.SET;
import static com.hartwig.pipeline.turquoise.PipelineSubjects.TYPE;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

public interface PipelineEvent extends TurquoiseEvent {

    PipelineSubjects commonSubjects();

    @Override
    default List<Subject> subjects() {
        List<Subject> subjects = Lists.newArrayList(Subject.of(commonSubjects().sample(), SAMPLE),
                Subject.of(commonSubjects().set(), SET),
                Subject.of(commonSubjects().type(), TYPE),
                Subject.of(commonSubjects().referenceBarcode(), BARCODE));
        subjects.addAll(commonSubjects().tumorBarcode().map(b -> Subject.of(b, BARCODE)).stream().collect(Collectors.toList()));
        subjects.addAll(commonSubjects().runId().map(r -> Subject.of(Integer.toString(r), RUN_ID)).stream().collect(Collectors.toList()));
        return subjects;
    }
}
