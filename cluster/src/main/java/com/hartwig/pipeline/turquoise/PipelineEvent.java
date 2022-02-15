package com.hartwig.pipeline.turquoise;

import static com.hartwig.pipeline.turquoise.PipelineProperties.BARCODE;
import static com.hartwig.pipeline.turquoise.PipelineProperties.RUN_ID;
import static com.hartwig.pipeline.turquoise.PipelineProperties.SAMPLE;
import static com.hartwig.pipeline.turquoise.PipelineProperties.SET;
import static com.hartwig.pipeline.turquoise.PipelineProperties.TYPE;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

public interface PipelineEvent extends TurquoiseEvent {

    PipelineProperties properties();

    @Override
    default List<Subject> subjects() {
        List<Label> labels = Lists.newArrayList(Label.of(SET, properties().set()));
        labels.addAll(properties().tumorBarcode().map(b -> Label.of(BARCODE, b)).stream().collect(Collectors.toList()));
        return List.of(Subject.of(SAMPLE, properties().sample(), labels));
    }

    @Override
    default List<Label> labels() {
        return Stream.concat(properties().runId().map(r -> Label.of(RUN_ID, Integer.toString(r))).stream(),
                Stream.of(Label.of(TYPE, properties().type()))).collect(Collectors.toList());
    }
}