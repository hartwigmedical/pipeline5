package com.hartwig.pipeline.alignment.bwa;

import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.MARK_DUPS;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.StringJoiner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class MarkDupsTest extends SubStageTest{

    @Override
    public SubStage createVictim() {
        return new MergeMarkDups(
                "tumor", TestInputs.REF_GENOME_37_RESOURCE_FILES,
                Lists.newArrayList("tumor.l001.bam", "tumor.l002.bam"));
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.bam";
    }

    @Test
    public void markDupsConsensus() {
        String sambamba = "/opt/tools/sambamba/0.6.8/sambamba";
        String samtools = "/opt/tools/samtools/1.14/samtools";

        // expected commands
        StringJoiner expectedCommands = new StringJoiner(" ");

        expectedCommands.add(
                toolCommand(MARK_DUPS)
                        + " -sample tumor "
                        + "-input_bam tumor.l001.bam,tumor.l002.bam "
                        + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-ref_genome_version V37 "
                        + "-unmap_regions /opt/resources/mappability/37/unmap_regions.37.tsv "
                        + "-form_consensus "
                        + "-multi_bam "
                        + "-sambamba " + sambamba
                        + " -samtools " + samtools
                        + " -output_bam /data/output/tumor.bam "
                        + "-output_dir /data/output "
                        + "-log_level DEBUG "
                        + "-threads $(grep -c '^processor' /proc/cpuinfo)");

        assertThat(bash(" ")).contains(expectedCommands.toString());
    }
}