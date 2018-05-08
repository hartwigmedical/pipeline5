package hmf.pipeline;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.junit.Test;

import hmf.bwa.Configuration;

public class PipelineTest {

    @Test
    public void pipelineProducesAlignedBAMWithExpectedSize() {
        JavaSparkContext context = new JavaSparkContext("local[1]", "pipelineTest");
        Pipeline victim = new Pipeline(context,
                Configuration.builder()
                        .samplePath(System.getProperty("user.dir") + "/src/test/resources/samples/CPCT12345678R")
                        .sampleName("CPCT12345678R_HJJLGCCXX_S1_L001")
                        .referenceFile(format("%s/reference_genome/reference.fa", System.getProperty("user.home")))
                        .build());
        List<GATKRead> execute = victim.execute();
        assertThat(execute).hasSize(17920);
    }
}