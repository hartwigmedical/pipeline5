package com.hartwig.pipeline.alignment.bwa;

import static com.hartwig.pipeline.alignment.bwa.Lanes.emptyBuilder;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.stages.SubStage;

import org.junit.Test;

public class LaneAlignmentTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new LaneAlignment(false,
                "reference.fasta",
                "COLO829v003R_AHHKYHDSXX_S13_L001_R1_001.fastq.gz",
                "COLO829v003R_AHHKYHDSXX_S13_L001_R2_001.fastq.gz",
                emptyBuilder().laneNumber("L001").flowCellId("flowCell").build());
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.flowCell-L001.bam";
    }

    @Test
    public void alignsBamsAndSortsEachLaneFastqPair() {
        assertThat(bash()).contains(
                "(/opt/tools/bwa/0.7.17/bwa mem -R " + "\"@RG\\tID:AHHKYHDSXX_S13_L001_001\\tLB:NA\\tPL:ILLUMINA\\tPU:flowCell\\tSM:NA\" "
                        + "-Y -t $(grep -c '^processor' /proc/cpuinfo) reference.fasta COLO829v003R_AHHKYHDSXX_S13_L001_R1_001.fastq.gz "
                        + "COLO829v003R_AHHKYHDSXX_S13_L001_R2_001.fastq.gz "
                        + "| /opt/tools/samtools/1.14/samtools view --no-PG --bam --uncompressed /dev/stdin "
                        + "| /opt/tools/sambamba/0.6.8/sambamba sort -o /data/output/tumor.flowCell-L001.bam /dev/stdin)");
    }
}