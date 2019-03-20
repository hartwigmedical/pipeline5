package com.hartwig.patient;

import org.immutables.value.Value;

@Value.Immutable
public interface Lane extends FileSystemEntity, Named {

    @Override
    String directory();

    @Override
    String name();

    String firstOfPairPath();

    String secondOfPairPath();

    String flowCellId();

    String index();

    String suffix();

    default String recordGroupId() {
        // KODU: Assumption is that name follows "sample_lane" format.
        String[] split = name().split("_");
        return String.format("%s_%s_%s_%s_%s", split[0], flowCellId(), index(), split[1], suffix());
    }

    static ImmutableLane.Builder builder() {
        return ImmutableLane.builder();
    }
}
