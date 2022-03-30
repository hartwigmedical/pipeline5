package com.hartwig.pipeline.resource;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.BashCommand;

import org.junit.Test;

public class TargetRegionsFilesTest
{

    @Test
    public void createsEmptyListIfNoTargetRegionsOverrideUrl() {
        assertThat(TargetRegionsFiles.overrides(
                Arguments.testDefaultsBuilder()
                        .targetRegionsDir(Optional.empty())
                        .build())).isEmpty();
    }

    @Test
    public void createsEmptyListIfNoTargetRegionsInImage() {
        assertThat(TargetRegionsCommand.overrides(Arguments.testDefaultsBuilder().targetRegionsBedLocation(Optional.of("target_regions"
                + ".bed")).build())).isEmpty();
    }

    @Test
    public void mksDirAndCopiesDownTargetRegionOverrides() {
        List<BashCommand> commands = TargetRegionsFiles.overrides(Arguments.testDefaultsBuilder()
                .targetRegionsDir(Optional.of("gs://bucket/target_regions_path"))
                .build());
        assertThat(commands).hasSize(2);
        assertThat(commands.stream().map(BashCommand::asBash)).containsExactly("mkdir -p /opt/resources/target_regions/override",
                "gsutil -qm cp -n gs://bucket/target_regions_path/* /opt/resources/target_regions/override");
    }
}