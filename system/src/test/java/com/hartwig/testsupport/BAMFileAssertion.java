package com.hartwig.testsupport;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.InputStream;

import com.hartwig.io.DataLocation;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;

import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

abstract class BAMFileAssertion {

    private final OutputType outputType;
    private final Sample sample;
    private final String resultDirectory;

    BAMFileAssertion(final String resultDirectory, final OutputType outputType, final Sample sample) {
        this.outputType = outputType;
        this.sample = sample;
        this.resultDirectory = resultDirectory;
    }

    void isEqualToExpected() {
        InputStream expected = Assertions.class.getResourceAsStream(String.format("/expected/%s", DataLocation.file(outputType, sample)));
        if (expected == null) {
            fail(format("No expected file found for sample [%s] and output [%s]. Check that the sample name is correct and there is a "
                    + "file in /src/test/resources/expected to verify against", sample.name(), outputType));
        }

        SamReaderFactory samReaderFactory = SamReaderFactory.make();
        SamReader samReaderExpected = samReaderFactory.open(SamInputResource.of(expected));
        SamReader samReaderResults = samReaderFactory.open(new File(DataLocation.path(resultDirectory, outputType, sample)));
        assertFile(samReaderExpected, samReaderResults);
    }

    String getName() {
        return sample.name();
    }

    OutputType getOutputType() {
        return outputType;
    }

    abstract void assertFile(SamReader expected, SamReader results);
}
