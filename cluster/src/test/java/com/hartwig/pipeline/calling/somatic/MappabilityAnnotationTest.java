package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class MappabilityAnnotationTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new MappabilityAnnotation("mappability.bed", "mappability.hdr");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.mappability.annotated.vcf.gz";
    }

    @Test
    public void runsBcfToolsMappabilityAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains("/data/tools/bcftools/1.3.1/bcftools annotate -a "
                + "mappability.bed -h mappability.hdr -c CHROM,FROM,TO,-,MAPPABILITY -o /data/output/tumor.mappability.annotated.vcf.gz "
                + "-O z /data/output/tumor.strelka.vcf");
    }

    @Test
    public void runsTabix() {
        assertThat(output.currentBash().asUnixString()).contains(
                "/data/tools/tabix/0.2.6/tabix /data/output/tumor.mappability.annotated.vcf.gz -p vcf");
    }
}