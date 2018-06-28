package com.hartwig.pipeline.runtime.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.testsupport.TestConfigurations;

import org.junit.Test;

public class ConfigurationReaderTest {

    @Test
    public void readPatientAndSparkFromTestYAML() throws Exception {
        Configuration configuration = YAMLConfigurationReader.from(System.getProperty("user.dir") + "/src/test/resources/");
        assertThat(configuration.patient().name()).isEqualTo(TestConfigurations.HUNDREDK_READS_HISEQ_PATIENT_NAME);
        assertThat(configuration.patient().directory()).isEqualTo("/patients");
        assertThat(configuration.patient().referenceGenomePath()).isEqualTo("/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa");
        assertThat(configuration.spark().get("master")).isEqualTo("local[2]");
        assertThat(configuration.spark().get("spark.property")).isEqualTo("value");
        assertThat(configuration.pipeline().flavour()).isEqualTo(Configuration.Flavour.ADAM);
    }
}