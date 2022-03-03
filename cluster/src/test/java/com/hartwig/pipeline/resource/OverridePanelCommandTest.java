package com.hartwig.pipeline.resource;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.BashCommand;

import org.junit.Test;

public class OverridePanelCommandTest {

    @Test
    public void createsEmptyListIfNoReferenceGenomeUrl() {
        assertThat(OverridePanelCommand.overrides(Arguments.testDefaultsBuilder().panelBedLocation(Optional.empty()).build())).isEmpty();
    }

    @Test
    public void mksDirAndCopiesDownReferenceGenome() {
        List<BashCommand> commands = OverridePanelCommand.overrides(Arguments.testDefaultsBuilder()
                .panelBedLocation(Optional.of("gs://bucket/path/panel.bed"))
                .build());
        assertThat(commands).hasSize(2);
        assertThat(commands.stream().map(BashCommand::asBash)).containsExactly("mkdir -p /opt/resources/panel/override",
                "gsutil -qm cp -n gs://bucket/path/panel.bed /opt/resources/panel/override");
    }
}