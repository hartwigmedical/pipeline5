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
    }

    @Test
    public void onlyMandatoryParametersReadAndOtherDefaulted() throws Exception {
        Configuration configuration = checkMandatory("/src/test/resources/configuration/only_mandatory");
        assertThat(configuration.spark().isEmpty());
        assertThat(configuration.pipeline().bwa().threads()).isEqualTo(12);
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