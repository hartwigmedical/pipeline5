package hmf.testsupport;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.InputStream;

import hmf.io.OutputFile;
import hmf.io.PipelineOutput;
import hmf.sample.HasSample;
import hmf.sample.Sample;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

abstract class BAMFileAssertion {

    private final PipelineOutput pipelineOutput;
    private final HasSample sample;

    BAMFileAssertion(final PipelineOutput pipelineOutput, final HasSample sample) {
        this.pipelineOutput = pipelineOutput;
        this.sample = sample;
    }

    void isEqualToExpected() {
        InputStream expected = Assertions.class.getResourceAsStream(format("/expected/%s", OutputFile.of(pipelineOutput, sample).file()));
        if (expected == null) {
            fail(format("No expected file found for sample [%s] and output [%s]. Check that the sample name is correct and there is a "
                    + "file in /src/test/resources/expected to verify against", sample.sample().name(), pipelineOutput));
        }

        SamReaderFactory samReaderFactory = SamReaderFactory.make();
        SamReader samReaderExpected = samReaderFactory.open(SamInputResource.of(expected));
        SamReader samReaderResults = samReaderFactory.open(new File(OutputFile.of(pipelineOutput, sample).path()));
        assertFile(samReaderExpected, samReaderResults);
    }

    PipelineOutput getPipelineOutput() {
        return pipelineOutput;
    }

    Sample getSample() {
        return sample.sample();
    }

    abstract void assertFile(SamReader expected, SamReader results);
}
