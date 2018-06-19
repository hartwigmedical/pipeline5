package com.hartwig.testsupport;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.InputStream;

import com.hartwig.io.OutputFile;
import com.hartwig.io.OutputType;
import com.hartwig.patient.FileSystemEntity;
import com.hartwig.patient.Named;

import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

abstract class BAMFileAssertion<T extends FileSystemEntity & Named> {

    private final OutputType outputType;
    private final T sample;

    BAMFileAssertion(final OutputType outputType, final T sample) {
        this.outputType = outputType;
        this.sample = sample;
    }

    void isEqualToExpected() {
        InputStream expected =
                Assertions.class.getResourceAsStream(String.format("/expected/%s", OutputFile.of(outputType, sample).file()));
        if (expected == null) {
            fail(format("No expected file found for sample [%s] and output [%s]. Check that the sample name is correct and there is a "
                    + "file in /src/test/resources/expected to verify against", sample.name(), outputType));
        }

        SamReaderFactory samReaderFactory = SamReaderFactory.make();
        SamReader samReaderExpected = samReaderFactory.open(SamInputResource.of(expected));
        SamReader samReaderResults = samReaderFactory.open(new File(OutputFile.of(outputType, sample).path()));
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
