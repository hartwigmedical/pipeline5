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

    private BAMFileAssertion(final String resultDirectory, final String suffix, final String sample) {
        this.sample = sample;
        this.resultDirectory = resultDirectory;
        this.suffix = suffix;
    }

    BAMFileAssertion(final String resultDirectory, final String sample) {
        this(resultDirectory, "", sample);
    }

    void isEqualToExpected() {
        String finalBamFile = sample + (suffix.isEmpty() ? "" : "." + suffix) + ".bam";
        String finalCramFile = finalBamFile.replaceAll("\\.bam$", ".cram");
        InputStream expected = Assertions.class.getResourceAsStream(String.format("/expected/%s", finalBamFile));
        if (expected == null) {
            fail(format("No expected file found for sample [%s]. Check that the sample name is correct and there is a "
                    + "file in /src/test/resources/expected to verify against", sample));
        }

        SamReaderFactory samReaderFactory = SamReaderFactory.make();
        SamReader samReaderExpected = samReaderFactory.open(SamInputResource.of(expected));
        SamReader samReaderResults = samReaderFactory.open(new File(resultDirectory + finalCramFile));
        assertFile(samReaderExpected, samReaderResults);
    }

    String getName() {
        return sample;
    }

    abstract void assertFile(SamReader expected, SamReader results);
}
