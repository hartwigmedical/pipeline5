package hmf.testsupport;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import hmf.pipeline.PipelineOutput;

public class BamAssertions {

    public static SampleFileAssertion assertThatOutput(String sampleName, PipelineOutput fileType) {
        return new SampleFileAssertion(sampleName, fileType);
    }

    public static class SampleFileAssertion {
        private final String sampleName;
        private final PipelineOutput fileType;

        SampleFileAssertion(final String sampleName, final PipelineOutput fileType) {
            this.sampleName = sampleName;
            this.fileType = fileType;
        }

        public void isEqualToExpected() throws IOException {
            InputStream expected = BamAssertions.class.getResourceAsStream(format("/expected/%s", fileType.file(sampleName)));
            String path = fileType.path(sampleName);
            InputStream actual = new FileInputStream(path);
            boolean contentIsEqual = IOUtils.contentEquals(expected, actual);
            assertThat(contentIsEqual).as("BAM files where not equal for sample %s and output %s", sampleName, fileType).isTrue();
        }
    }
}
