package com.hartwig.pipeline.smoke;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.testsupport.Resources;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

public class ComparAssert extends AbstractAssert<ComparAssert, File> {
    private final File localCopyOfBucket;
    private final String runName;
    private final File outputDir;

    public ComparAssert(final File file, final String runName) {
        super(file, ComparAssert.class);
        localCopyOfBucket = file;
        this.runName = runName;
        try {
            outputDir = java.nio.file.Files.createTempDirectory("smoketest-compar-").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialise Compar output directory", e);
        }
    }

    public static ComparAssert assertThat(final Storage storage, final String outputBucket, final String runName) {
        return new ComparAssert(new BucketRun(storage, outputBucket, runName).download(), runName);
    }

    public ComparAssert isEqualToTruthset(final String truthSet) {
        try {
            new ComparWrapper().run(new File(localCopyOfBucket, runName), new File(truthSet), outputDir);
        } catch (ParseException e) {
            throw new RuntimeException("Failed to run Compar", e);
        }
        File noDifferences = new File(Resources.testResource("smoke_test/compar_no_differences"));
        for (File expected : noDifferences.listFiles()) {
            File actual = Path.of(outputDir.getAbsolutePath(), expected.getName().replaceAll("^sample", "COLO829v003T")).toFile();
            Assertions.assertThat(readContents(actual))
                    .as(format("Actual contents do not match expectatation in [%s]", expected.getAbsolutePath()))
                    .isEqualTo(readContents(expected));
        }
        return this;
    }

    public void cleanup() {
        try {
            FileUtils.deleteDirectory(localCopyOfBucket);
            FileUtils.deleteDirectory(outputDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to cleanup after Compar run", e);
        }
    }

    private String readContents(final File source) {
        try {
            return Files.readString(Path.of(source.getPath()), Charset.defaultCharset());
        } catch (IOException e) {
            Assertions.fail("Unable to read contents of [%s]", source.getAbsolutePath());
            return null;
        }
    }

    private class ComparWrapper {
        void run(final File victim, final File truthset, final File outputDir) throws ParseException {

            List<String> arguments = List.of(
                    "-match_level",
                    "REPORTABLE", // KEY_FIELDS
                    "-categories",
                    "PURPLE,LINX,LILAC,CUPPA",
                    "-sample",
                    "COLO829v003T",
                    "-output_dir",
                    outputDir.getAbsolutePath(),
                    "-sample_dir_ref",
                    truthset.getAbsolutePath(),
                    "-sample_dir_new",
                    victim.getAbsolutePath());

            com.hartwig.hmftools.compar.Compar.main(arguments.toArray(new String[] {}));
        }
    }
}
