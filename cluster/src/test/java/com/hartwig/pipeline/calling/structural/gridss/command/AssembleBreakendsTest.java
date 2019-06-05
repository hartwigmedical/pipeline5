package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class AssembleBreakendsTest implements CommonEntities {
    private AssembleBreakends command;

    @Before
    public void setup() {
        command = new AssembleBreakends(REFERENCE_BAM, TUMOR_BAM, REFERENCE_GENOME);
    }

    @Test
    public void shouldReturnClassName() {
        assertThat(command.className()).isEqualTo("gridss.AssembleBreakends");
    }

    @Test
    public void shouldUse31GigabytesOfHeap() {
        assertThat(command.memoryGb()).isEqualTo(31);
    }

    @Test
    public void shouldConstructGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments(ARGS_TMP_DIR)
                .and(ARG_KEY_WORKING_DIR, OUT_DIR)
                .and(ARGS_REFERENCE_SEQUENCE)
                .and(ARG_KEY_INPUT, REFERENCE_BAM)
                .and(ARG_KEY_INPUT, TUMOR_BAM)
                .and(ARG_KEY_OUTPUT, command.assemblyBam())
                .and(ARG_KEY_WORKER_THREADS, "2")
                .and(ARGS_BLACKLIST)
                .and(ARGS_GRIDSS_CONFIG)
                .andNoMore();
    }

    @Test
    public void shouldReturnAssemblyBamPath() {
        assertThat(command.assemblyBam()).isEqualTo(format("%s/reference-tumor.assembly.bam", OUT_DIR));
    }
}
