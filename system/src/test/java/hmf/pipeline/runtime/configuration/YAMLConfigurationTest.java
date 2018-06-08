package hmf.pipeline.runtime.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import hmf.pipeline.Configuration;
import hmf.testsupport.TestPatients;

public class YAMLConfigurationTest {

    @Test
    public void readPatientAndSparkFromTestYAML() throws Exception {
        Configuration configuration = YAMLConfiguration.from(System.getProperty("user.dir") + "/src/test/resources/");
        assertThat(configuration.patientName()).isEqualTo(TestPatients.HUNDREDK_READS_HISEQ_PATIENT_NAME);
        assertThat(configuration.patientDirectory()).isEqualTo("/patients");
        assertThat(configuration.referencePath()).isEqualTo("/reference/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa");
        assertThat(configuration.sparkMaster()).isEqualTo("local[2]");
    }
}