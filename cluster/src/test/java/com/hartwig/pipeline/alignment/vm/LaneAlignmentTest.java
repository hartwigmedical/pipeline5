package com.hartwig.pipeline.alignment.vm;

import static com.hartwig.pipeline.alignment.vm.Lanes.emptyBuilder;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

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
        return "/data/output/tumor.sorted.L001.bam";
    }

    @Test
    public void alignsBamsAndSortsEachLaneFastqPair() {
        assertThat(bash()).contains("(/opt/tools/bwa/0.7.17/bwa mem -R "
                + "\"@RG\\tID:tumor___L001_\\tLB:tumor\\tPL:ILLUMINA\\tPU:\\tSM:tumor\" -Y -t $(grep -c '^processor' /proc/cpuinfo) "
                + "reference.fasta R1.fastq R2.fastq | /opt/tools/sambamba/0.6.8/sambamba view -f bam -S -l0 /dev/stdin | "
                + "/opt/tools/sambamba/0.6.8/sambamba sort -o /data/output/tumor.sorted.L001.bam /dev/stdin)");
    }
}