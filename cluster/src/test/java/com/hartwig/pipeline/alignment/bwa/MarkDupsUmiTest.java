package com.hartwig.pipeline.alignment.bwa;

import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.MARK_DUPS;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class MarkDupsUmiTest extends SubStageTest{

    @Override
    public SubStage createVictim() {
        return new MergeMarkDups(
                "tumor", TestInputs.REF_GENOME_37_RESOURCE_FILES,
                Lists.newArrayList("tumor.l001.bam", "tumor.l002.bam"), true);
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.bam";
    }

    @Test
    public void markDupsUmiConsensus() {
        String sambamba = "/opt/tools/sambamba/0.6.8/sambamba";
        String samtools = "/opt/tools/samtools/1.14/samtools";

        // expected commands
        List<String> expectedCommands = ImmutableList.of(
                sambamba + " merge -t $(grep -c '^processor' /proc/cpuinfo) /data/output/tumor.raw.bam tumor.l001.bam tumor.l002.bam"
                        + toolCommand(MARK_DUPS)
                        + " -sample tumor "
                        + "-bam_file /data/output/tumor.raw.bam "
                        + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-ref_genome_version V37 "
                        + "-form_consensus "
                        + "-output_dir /data/output "
                        + "-log_level DEBUG "
                        + "-threads $(grep -c '^processor' /proc/cpuinfo)"
                        + samtools + " sort -@ $(grep -c '^processor' /proc/cpuinfo) -m 2G -T tmp -O bam /data/output/tumor.mark_dups.bam -o /data/output/tumor.bam"
                        + samtools + " index -@ $(grep -c '^processor' /proc/cpuinfo) /data/output/tumor.bam"
                        + "rm /data/output/tumor.raw.bam /data/output/tumor.raw.bam.bai /data/output/tumor.mark_dups.bam");

        assertThat(bash()).contains(expectedCommands);
    }
}