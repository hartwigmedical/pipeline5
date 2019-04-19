package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class CombineVcfsCommandTest {

    @Test
    public void createsBashForCombineVcfsGatkAnalysis() {
        CombineVcfsCommand victim =
                new CombineVcfsCommand("reference/genome/path.fasta", "/data/output/snvs.vcf", "/data/output/indels.vcf", "/data/output");
        assertThat(victim.asBash()).isEqualTo("java -Xmx20G -jar /data/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T CombineVariants "
                + "-R reference/genome/path.fasta --genotypemergeoption unsorted -V:snvs /data/output/snvs.vcf -V:indels "
                + "/data/output/indels.vcf -o /data/output/combined.vcf");
    }
}