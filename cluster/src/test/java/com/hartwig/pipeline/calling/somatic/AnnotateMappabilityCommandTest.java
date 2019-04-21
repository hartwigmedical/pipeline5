package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class AnnotateMappabilityCommandTest {

    @Test
    public void createsBashToAnnotateMappabilityWithBcfTools() {
        AnnotateMappabilityCommand victim = new AnnotateMappabilityCommand("mappability.bed.gz", "mappability.hdr", "combined.vcf",
                "/data/output/mappability.annotated.vcf");
        assertThat(victim.asBash()).isEqualTo("/data/tools/bcftools/1.3.1/bcftools annotate -a mappability.bed.gz -h mappability.hdr -c "
                + "CHROM,FROM,TO,-,MAPPABILITY -o /data/output/mappability.annotated.vcf -O z combined.vcf");
    }
}