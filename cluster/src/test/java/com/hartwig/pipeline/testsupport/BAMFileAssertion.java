package com.hartwig.pipeline.testsupport;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.InputStream;

import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

abstract class BAMFileAssertion {

    private final String sample;
    private final String resultDirectory;
    private final String suffix;

    private BAMFileAssertion(String resultDirectory, String suffix, String sample) {
        this.sample = sample;
        this.resultDirectory = resultDirectory;
        this.suffix = suffix;
    }
    BAMFileAssertion(String resultDirectory, String sample) {
        this(resultDirectory, "", sample);
    }

    void isEqualToExpected() {
        String finalBamFile = sample + (suffix.isEmpty() ? "" : "." + suffix) + ".bam";
        InputStream expected = Assertions.class.getResourceAsStream(String.format("/expected/%s", finalBamFile));
        if (expected == null) {
            fail(format("No expected file found for sample [%s]. Check that the sample name is correct and there is a "
                    + "file in /src/test/resources/expected to verify against", sample));
        }

        SamReaderFactory samReaderFactory = SamReaderFactory.make();
        SamReader samReaderExpected = samReaderFactory.open(SamInputResource.of(expected));
        SamReader samReaderResults = samReaderFactory.open(new File(resultDirectory + finalBamFile));
        assertFile(samReaderExpected, samReaderResults);
    }

    String getName() {
        return sample;
    }

    abstract void assertFile(SamReader expected, SamReader results);
}
