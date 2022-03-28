package com.hartwig.pipeline.resource;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.BashCommand;

import org.junit.Test;

public class TargetRegionsCommandTest {

    @Test
    public void createsEmptyListIfNoTargetRegionsOverrideUrl() {
        assertThat(TargetRegionsCommand.overrides(Arguments.testDefaultsBuilder().targetRegionsBedLocation(Optional.empty()).build())).isEmpty();
    }

    @Test
    public void createsEmptyListIfNoTargetRegionsInImage() {
        assertThat(TargetRegionsCommand.overrides(Arguments.testDefaultsBuilder().targetRegionsBedLocation(Optional.of("target_regions"
                + ".bed")).build())).isEmpty();
    }

    @Test
    public void mksDirAndCopiesDownTargetRegionOverrides() {
        List<BashCommand> commands = TargetRegionsCommand.overrides(Arguments.testDefaultsBuilder()
                .targetRegionsBedLocation(Optional.of("gs://bucket/path/target_regions.bed"))
                .build());
        assertThat(commands).hasSize(2);
        assertThat(commands.stream().map(BashCommand::asBash)).containsExactly("mkdir -p /opt/resources/target_regions/override",
                "gsutil -qm cp -n gs://bucket/path/target_regions.bed /opt/resources/target_regions/override");
    }
}