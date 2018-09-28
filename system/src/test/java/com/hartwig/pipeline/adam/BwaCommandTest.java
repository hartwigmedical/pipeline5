package com.hartwig.pipeline.adam;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Collectors;

import com.hartwig.patient.ImmutableReferenceGenome;
import com.hartwig.patient.Sample;
import com.hartwig.testsupport.Lanes;

import org.junit.Test;

public class BwaCommandTest {

    @Test
    public void commandMatchesOldPipelineArguments() {
        String victim = BwaCommand.tokens(ImmutableReferenceGenome.of("reference.fa"), Sample.builder("", "SAMPLE").build(),
                Lanes.emptyBuilder().readsPath("reads1.fastq").name("SAMPLE_L001").index("S1").suffix("001").flowCellId("FLOWCELL").build(),
                12).stream().collect(Collectors.joining(" "));
        assertThat(victim).isEqualTo(
                "bwa mem -p -R @RG\\tID:SAMPLE_FLOWCELL_S1_L001_001\\tLB:SAMPLE\\tPL:ILLUMINA\\tPU:FLOWCELL\\tSM:SAMPLE -c 100 -t 12 reference.fa -");
    }
}