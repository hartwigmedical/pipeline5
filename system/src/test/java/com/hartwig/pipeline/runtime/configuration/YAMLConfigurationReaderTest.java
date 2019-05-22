package com.hartwig.pipeline.runtime.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import com.hartwig.testsupport.TestConfigurations;

import org.junit.Test;

public class YAMLConfigurationReaderTest {

    @Test
    public void allParametersReadFromYAMLFile() throws Exception {
        Configuration configuration = checkMandatory("all_parameters");
        assertThat(configuration.spark().get("spark.property")).isEqualTo("value");
        assertThat(configuration.pipeline().bwa().threads()).isEqualTo(5);
        assertThat(configuration.pipeline().hdfs()).isEqualTo("hdfs://localhost:8020");
        assertThat(configuration.pipeline().saveResultsAsSingleFile()).isFalse();
        assertThat(configuration.patient().name()).isEqualTo(TestConfigurations.HUNDREDK_READS_HISEQ_PATIENT_NAME);
    }

    @Test
    public void onlyMandatoryParametersReadAndOtherDefaulted() throws Exception {
        Configuration configuration = checkMandatory("only_mandatory");
        assertThat(configuration.spark().isEmpty());
        assertThat(configuration.pipeline().bwa().threads()).isEqualTo(12);
        assertThat(configuration.pipeline().hdfs()).isEqualTo("file:///");
        assertThat(configuration.pipeline().saveResultsAsSingleFile()).isFalse();
        assertThat(configuration.patient().name()).isEmpty();
    }

    private static Configuration checkMandatory(final String confDirectory) throws IOException {
        Configuration configuration = YAMLConfigurationReader.from(System.getProperty("user.dir") + "/src/test/resources/configuration/" + confDirectory);
        assertThat(configuration.patient().directory()).isEqualTo("/patients");
        assertThat(configuration.referenceGenome().path()).isEqualTo("/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa");
        assertThat(configuration.spark().get("master")).isEqualTo("local[2]");
        return configuration;
    }
}