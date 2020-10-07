package com.hartwig.pipeline.resource;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.BashCommand;

import org.junit.Test;

public class OverrideReferenceGenomeCommandTest {

    @Test
    public void createsEmptyListIfNoReferenceGenomeUrl() {
        assertThat(OverrideReferenceGenomeCommand.overrides(Arguments.testDefaultsBuilder()
                .refGenomeUrl(Optional.empty())
                .build())).isEmpty();
    }

    @Test
    public void mksDirAndCopiesDownReferenceGenome() {
        List<BashCommand> commands = OverrideReferenceGenomeCommand.overrides(Arguments.testDefaultsBuilder()
                .refGenomeUrl(Optional.of("gs://bucket/path/reference.bam"))
                .build());
        assertThat(commands).hasSize(2);
        assertThat(commands.stream().map(BashCommand::asBash)).containsExactly("mkdir -p /opt/resources/reference_genome/override",
                "gsutil -qm cp -n gs://bucket/path/reference* /opt/resources/reference_genome/override");
    }
}