package com.hartwig.testsupport;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.InputStream;

import com.hartwig.patient.Sample;

import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

abstract class BAMFileAssertion {

    private final Sample sample;
    private final String resultDirectory;

    BAMFileAssertion(final String resultDirectory, final Sample sample) {
        this.sample = sample;
        this.resultDirectory = resultDirectory;
    }

    void isEqualToExpected() {
        String finalBamFile = sample.name() + ".bam";
        InputStream expected = Assertions.class.getResourceAsStream(String.format("/expected/%s", finalBamFile));
        if (expected == null) {
            fail(format("No expected file found for sample [%s]. Check that the sample name is correct and there is a "
                    + "file in /src/test/resources/expected to verify against", sample.name()));
        }

        SamReaderFactory samReaderFactory = SamReaderFactory.make();
        SamReader samReaderExpected = samReaderFactory.open(SamInputResource.of(expected));
        SamReader samReaderResults = samReaderFactory.open(new File(resultDirectory + finalBamFile));
        assertFile(samReaderExpected, samReaderResults);
    }

    String getName() {
        return sample.name();
    }

    abstract void assertFile(SamReader expected, SamReader results);
}
