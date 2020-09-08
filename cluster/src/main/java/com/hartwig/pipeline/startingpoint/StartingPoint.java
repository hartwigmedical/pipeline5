package com.hartwig.pipeline.startingpoint;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.somatic.SageCaller;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.purple.Purple;

import org.jetbrains.annotations.NotNull;

public class StartingPoint {

    private final StartingPoints startingPoint;

    enum StartingPoints {
        BEGINNING(Collections.emptyList()),
        ALIGNMENT_COMPLETE(List.of(Aligner.NAMESPACE, BamMetrics.NAMESPACE, GermlineCaller.NAMESPACE)),
        CALLING_COMPLETE(concat(ALIGNMENT_COMPLETE.namespaces,
                List.of(SageCaller.NAMESPACE, StructuralCaller.NAMESPACE, Cobalt.NAMESPACE, Amber.NAMESPACE))),
        PURPLE_COMPLETE(concat(CALLING_COMPLETE.namespaces, List.of(Purple.NAMESPACE)));

        private List<String> namespaces;

        StartingPoints(final List<String> namespaces) {
            this.namespaces = namespaces;
        }

        static List<String> concat(List<String> first, List<String> second) {
            return ImmutableList.<String>builder().addAll(first).addAll(second).build();
        }
    }

    public StartingPoint(final Arguments arguments) {
        this.startingPoint = arguments.startingPoint().map(String::toUpperCase).map(toEnum()).orElse(StartingPoints.BEGINNING);
    }

    @NotNull
    public static Function<String, StartingPoints> toEnum() {
        return s -> {
            try {
                return StartingPoints.valueOf(s);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("[%s] is not a valid starting point. [%s] are accepted",
                        s,
                        Stream.of(StartingPoints.values()).map(StartingPoints::toString).collect(Collectors.joining(","))));
            }
        };
    }

    public boolean usePersisted(String namespace) {
        return startingPoint.namespaces.contains(namespace);
    }
}
