package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.stages.SubStage;

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
        assertThat(bash()).contains("/opt/tools/bcftools/1.9/bcftools annotate -a "
                + "mappability.bed -h mappability.hdr -c CHROM,FROM,TO,-,MAPPABILITY /data/output/tumor.strelka.vcf "
                + "-O z -o /data/output/tumor.mappability.annotated.vcf.gz");
    }

    @Test
    public void runsTabix() {
        assertThat(bash()).contains(
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.mappability.annotated.vcf.gz -p vcf");
    }
}