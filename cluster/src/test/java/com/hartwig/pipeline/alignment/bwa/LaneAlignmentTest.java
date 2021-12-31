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
                "tumor",
                emptyBuilder().laneNumber("L001").flowCellId("flowCell").build());

    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.flowCell-L001.bam";
    }

    @Test
    public void alignsBamsAndSortsEachLaneFastqPair() {
        assertThat(bash()).contains("(/opt/tools/bwa/0.7.17/bwa mem -R "
                + "\"@RG\\tID:COLO829v003R_AHHKYHDSXX_S13_L001_001\\tLB:tumor\\tPL:ILLUMINA\\tPU:flowCell\\tSM:tumor\" "
                + "-Y -t $(grep -c '^processor' /proc/cpuinfo) -K 10000000 reference.fasta COLO829v003R_AHHKYHDSXX_S13_L001_R1_001.fastq.gz "
                + "COLO829v003R_AHHKYHDSXX_S13_L001_R2_001.fastq.gz | /opt/tools/sambamba/0.6.8/sambamba view -f bam -S -l0 /dev/stdin "
                + "| /opt/tools/sambamba/0.6.8/sambamba sort -o /data/output/tumor.flowCell-L001.bam /dev/stdin)");
    }
}