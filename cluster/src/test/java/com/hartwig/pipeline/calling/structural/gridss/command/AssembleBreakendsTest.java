package com.hartwig.pipeline.calling.structural.gridss.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;

import org.junit.Before;
import org.junit.Test;

public class AssembleBreakendsTest implements CommonEntities {
    private AssembleBreakends command;
    private String configurationFile;
    private String blacklist;
    private String assemblyBam;

    @Before
    public void setup() {
        configurationFile = "/some/path/config.properties";
        blacklist = "/blacklist.bed";
        assemblyBam = "/some/path/to/output.bam";
        command = new AssembleBreakends(REFERENCE_BAM, TUMOR_BAM, assemblyBam, REFERENCE_GENOME,
                configurationFile, blacklist);
    }

    @Test
    public void shouldReturnClassName() {
        assertThat(command.className()).isEqualTo("gridss.AssembleBreakends");
    }

    @Test
    public void shouldUseSpecificAmountOfHeap() {
        assertThat(command.memoryGb()).isEqualTo(80);
    }

    @Test
    public void shouldConstructGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments(ARGS_TMP_DIR)
                .and(ARG_KEY_WORKING_DIR, OUT_DIR)
                .and(ARGS_REFERENCE_SEQUENCE)
                .and(ARG_KEY_INPUT, REFERENCE_BAM)
                .and(ARG_KEY_INPUT, TUMOR_BAM)
                .and(ARG_KEY_OUTPUT, assemblyBam)
                .andBlacklist(blacklist)
                .andConfigFile(configurationFile)
                .andNoMore();
    }
}
