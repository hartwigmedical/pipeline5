package com.hartwig.pipeline.smoke;

import static java.lang.String.format;

import static com.hartwig.pipeline.resource.RefGenomeVersion.V37;
import static com.hartwig.pipeline.smoke.SmokeTest.FILE_ENCODING;
import static com.hartwig.pipeline.smoke.SmokeTest.INPUT_MODE_TUMOR_REF;
import static com.hartwig.pipeline.smoke.SmokeTest.findCloudSdk;
import static com.hartwig.pipeline.smoke.SmokeTest.listOutput;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ImmutableArguments;
import com.hartwig.pipeline.storage.StorageProvider;
import com.hartwig.pipeline.testsupport.Resources;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.jetbrains.annotations.NotNull;

public class ComparSmokeTester {
    private static final String PIPELINE_BUCKET = "pipeline_bucket";
    private static final String LOCAL_DIR = "local_dir";
    private static final String RUN_TAG = "run_tag";
    private static final String SAMPLE_ID = "COLO829v003";
    private final List<String> expectedFiles;
    private final String pipelineBucket;
    private final String localDir;
    private final String runTag;

    public ComparSmokeTester(final CommandLine cmd) {
        expectedFiles = Lists.newArrayList();
        loadExpectedFiles();

        pipelineBucket = cmd.getOptionValue(PIPELINE_BUCKET);
        localDir = cmd.getOptionValue(LOCAL_DIR);
        runTag = cmd.getOptionValue(RUN_TAG);
    }

    private static String fixtureDir() {
        return "smoke_test/" + INPUT_MODE_TUMOR_REF + File.separator;
    }

    private static String pipelineTruthsetDir() {
        return fixtureDir() + "truthset";
    }

    public static void main(@NotNull final String[] args) throws ParseException {
        Options options = new Options();

        options.addOption(PIPELINE_BUCKET, true, "Pipeline bucket");
        options.addOption(LOCAL_DIR, true, "Local directory to for pipeline results");
        options.addOption(RUN_TAG, true, "Run tag to pull pipeline results from");

        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);

        Configurator.setRootLevel(Level.DEBUG);

        ComparSmokeTester comparSmokeTester = new ComparSmokeTester(cmd);
        comparSmokeTester.run();
    }

    public void run() {

        if (pipelineBucket != null) {
            testBucketPipelineResults();
        }

        if (localDir != null) {
            testLocalPipelineResults();
        }
    }

    private void loadExpectedFiles() {

        try {
            File expectedFilesResource = new File(Resources.testResource(fixtureDir() + "expected_output_files"));

            expectedFiles.addAll(FileUtils.readLines(expectedFilesResource, FILE_ENCODING));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private String setName() {
        return format("%s-%s", SAMPLE_ID, runTag);
    }

    private void testBucketPipelineResults() {

        String setName = setName();
        String whoami = System.getProperty("user.name");

        final ImmutableArguments.Builder builder = Arguments.defaultsBuilder(Arguments.DefaultsProfile.DEVELOPMENT.toString())
                .cleanup(false)
                .context(Pipeline.Context.PLATINUM)
                .sampleJson("")
                .cloudSdkPath(findCloudSdk(whoami))
                .runTag(runTag)
                .runGermlineCaller(false)
                .outputBucket(pipelineBucket)
                .useTargetRegions(false)
                .refGenomeVersion(V37);

        Arguments arguments = builder.build();

        try {
            Storage storage = StorageProvider.from(arguments, GoogleCredentials.getApplicationDefault()).get();
            List<String> actualFiles = listOutput(setName, arguments.outputBucket(), storage);
            assertThat(actualFiles).containsOnlyElementsOf(expectedFiles);

            ComparAssert.assertThat(storage, arguments.outputBucket(), setName)
                    .isEqualToTruthset(Resources.testResource(pipelineTruthsetDir()))
                    .cleanup();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void testLocalPipelineResults() {
        String setName = setName();

        ComparAssert localPipelineResults = new ComparAssert(new File(localDir), setName);

        String truthSet = Resources.testResource(pipelineTruthsetDir());

        localPipelineResults.isEqualToTruthset(truthSet);
    }
}
