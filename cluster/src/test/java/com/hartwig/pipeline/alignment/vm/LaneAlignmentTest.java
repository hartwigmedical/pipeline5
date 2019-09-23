package com.hartwig.pipeline.alignment.vm;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.alignment.vm.Lanes.emptyBuilder;
import static com.hartwig.pipeline.testsupport.TestConstants.*;
import static org.assertj.core.api.Assertions.assertThat;

public class LaneAlignmentTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new LaneAlignment("reference.fasta",
                "R1.fastq",
                "R2.fastq",
                "tumor",
                emptyBuilder().name("tumor_L001").laneNumber("L001").build());

    }

    @Override
    public String expectedPath() {
        return outFile( "tumor.sorted.L001.bam");
    }

    @Test
    public void alignsBamsAndSortsEachLaneFastqPair() {
        assertThat(output.currentBash().asUnixString()).contains("(" + TOOLS_BWA + " mem -R "
                + "\"@RG\\tID:tumor___L001_\\tLB:tumor\\tPL:ILLUMINA\\tPU:\\tSM:tumor\" -Y -t " + PROC_COUNT
                + " reference.fasta R1.fastq R2.fastq | " + TOOLS_SAMBAMBA + " view -f bam -S -l0 /dev/stdin | "
                + TOOLS_SAMBAMBA + " sort -o " + expectedPath() + " /dev/stdin)");
    }
}