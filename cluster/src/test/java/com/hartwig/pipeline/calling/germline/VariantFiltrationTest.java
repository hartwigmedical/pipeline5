package com.hartwig.pipeline.calling.germline;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_GATK_JAR;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class VariantFiltrationTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new VariantFiltration("snps", ImmutableMap.of("filter1", "QD < 2.0", "filter2", "MQ < 10.0"), "reference.fasta");
    }

    @Override
    public String expectedPath() {
        return outFile("reference.filtered_snps.vcf");
    }

    @Test
    public void filtersVariantsWithGatk() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx20G -jar " + TOOLS_GATK_JAR + " -T "
                + "VariantFiltration -R reference.fasta -V " + outFile("reference.strelka.vcf") + " -o "
                + expectedPath() + " --filterExpression \"QD < 2.0\" --filterName \"filter1\" --filterExpression "
                + "\"MQ < 10.0\" --filterName \"filter2\" --clusterSize 3 --clusterWindowSize 35 ");
    }

    @Override
    protected String sampleName() {
        return "reference";
    }
}