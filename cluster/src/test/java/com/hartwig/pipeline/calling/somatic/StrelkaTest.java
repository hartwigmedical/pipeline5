package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class StrelkaTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new Strelka("reference.bam", "tumor.bam", "strelka.config", "reference_genome.fasta");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.strelka.vcf";
    }


    @Test
    public void runsConfigureStrelkaWorkflow() {
        assertThat(output.currentBash().asUnixString()).contains("/opt/tools/strelka/1.0.14/bin/configureStrelkaWorkflow.pl "
                + "--tumor tumor.bam --normal reference.bam --config strelka.config --ref reference_genome.fasta "
                + "--output-dir /data/output/strelkaAnalysis");
    }

    @Test
    public void runsStrelkaMakefile() {
        assertThat(output.currentBash().asUnixString()).contains("make -C /data/output/strelkaAnalysis "
                + "-j $(grep -c '^processor' /proc/cpuinfo) >>/data/output/run.log");
    }

    @Test
    public void runsGatkCombineVcf() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx20G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar "
                + "-T CombineVariants -R reference_genome.fasta --genotypemergeoption unsorted "
                + "-V:snvs /data/output/strelkaAnalysis/results/passed.somatic.snvs.vcf -V:indels "
                + "/data/output/strelkaAnalysis/results/passed.somatic.indels.vcf -o /data/output/tumor.strelka.vcf");
    }
}