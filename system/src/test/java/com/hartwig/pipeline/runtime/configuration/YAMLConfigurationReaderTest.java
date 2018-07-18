package com.hartwig.pipeline.runtime.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import com.hartwig.testsupport.TestConfigurations;

import org.junit.Test;

public class YAMLConfigurationReaderTest {

    @Test
    public void allParametersReadFromYAMLFile() throws Exception {
        Configuration configuration = checkMandatory("/src/test/resources/configuration/all_parameters");
        assertThat(configuration.spark().get("spark.property")).isEqualTo("value");
        assertThat(configuration.pipeline().bwa().threads()).isEqualTo(5);
        assertThat(configuration.patient().knownIndelPaths()).containsExactly("/data/dbs/GATK_bundle_v2.8/1000G_phase1.indels.b37.vcf",
                "/data/dbs/GATK_bundle_v2.8/Mills_and_1000G_gold_standard.indels.b37.vcf");
        assertThat(configuration.pipeline().callGermline()).isTrue();
    }

    @Test
    public void onlyMandatoryParametersReadAndOtherDefaulted() throws Exception {
        Configuration configuration = checkMandatory("/src/test/resources/configuration/only_mandatory");
        assertThat(configuration.spark().isEmpty());
        assertThat(configuration.pipeline().bwa().threads()).isEqualTo(12);
        assertThat(configuration.pipeline().callGermline()).isFalse();
    }

    private static Configuration checkMandatory(final String confDirectory) throws IOException {
        Configuration configuration = YAMLConfigurationReader.from(System.getProperty("user.dir") + confDirectory);
        assertThat(configuration.patient().name()).isEqualTo(TestConfigurations.HUNDREDK_READS_HISEQ_PATIENT_NAME);
        assertThat(configuration.patient().directory()).isEqualTo("/patients");
        assertThat(configuration.patient().referenceGenomePath()).isEqualTo("/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa");
        assertThat(configuration.spark().get("master")).isEqualTo("local[2]");
        assertThat(configuration.pipeline().flavour()).isEqualTo(Configuration.Flavour.ADAM);
        return configuration;
    }
}