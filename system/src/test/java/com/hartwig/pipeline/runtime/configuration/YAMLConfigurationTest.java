package com.hartwig.pipeline.runtime.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Configuration;
import com.hartwig.testsupport.TestConfigurations;

import org.junit.Test;

public class YAMLConfigurationTest {

    @Test
    public void readPatientAndSparkFromTestYAML() throws Exception {
        Configuration configuration = YAMLConfiguration.from(System.getProperty("user.dir") + "/src/test/resources/");
        assertThat(configuration.patientName()).isEqualTo(TestConfigurations.HUNDREDK_READS_HISEQ_PATIENT_NAME);
        assertThat(configuration.patientDirectory()).isEqualTo("/patients");
        assertThat(configuration.referenceGenomePath()).isEqualTo("/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa");
        assertThat(configuration.sparkMaster()).isEqualTo("local[2]");
        assertThat(configuration.sparkProperties().get("spark.property")).isEqualTo("value");
        assertThat(configuration.flavour()).isEqualTo(Configuration.Flavour.ADAM);
        assertThat(configuration.persistIntermediateResults()).isEqualTo(true);
    }
}