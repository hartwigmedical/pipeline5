package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.calling.substages.CosmicAnnotation;

import org.junit.Test;

public class CosmicAnnotationTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new CosmicAnnotation("cosmic.vcf.gz", "ID,INFO");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.cosmic.annotated.vcf.gz";
    }

    @Test
    public void runsBcfToolsDbSnpAnnotation() {
        assertThat(bash()).contains(
                "/opt/tools/bcftools/1.3.1/bcftools annotate -a cosmic.vcf.gz -c ID,INFO "
                        + "-o /data/output/tumor.cosmic.annotated.vcf.gz -O z /data/output/tumor.strelka.vcf");
    }

    @Test
    public void runsTabix() {
        assertThat(bash()).contains(
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.cosmic.annotated.vcf.gz -p vcf ");
    }

}