package com.hartwig.pipeline.smoke;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.events.Pipeline.Context;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ImmutableArguments;
import com.hartwig.pipeline.PipelineMain;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.storage.StorageProvider;
import com.hartwig.pipeline.testsupport.Resources;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(Parallelized.class)
@Category(value = IntegrationTest.class)
public class SmokeTest {

    private static final String FILE_ENCODING = "UTF-8";
    private static final String STAGED_FLAG_FILE = "STAGED";
    private static final String CLOUD_SDK_PATH = "/root/google-cloud-sdk/bin";
    private File resultsDir;
    private String whoami;

    @Before
    public void setUp() throws Exception {
        resultsDir = new File(workingDir() + "/results");
        //noinspection ResultOfMethodCallIgnored
        resultsDir.mkdir();
        whoami = System.getProperty("user.name");
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(resultsDir);
    }

    @Test
    public void tumorReference() throws Exception {
        runFullPipelineAndCheckFinalStatus("tumor-reference", PipelineStatus.QC_FAILED);
    }

    @Test
    public void tumorOnly() throws Exception {
        runFullPipelineAndCheckFinalStatus("tumor", PipelineStatus.SUCCESS);
    }

    @Test
    public void referenceOnly() throws Exception {
        runFullPipelineAndCheckFinalStatus("reference", PipelineStatus.SUCCESS);
    }

    @Ignore
    @Test
    public void tumorOnlyWithTargetedRegions() throws Exception {
        runFullPipelineAndCheckFinalStatus("tumor",
                PipelineStatus.SUCCESS,
                Optional.of("target_regions_definition.38.bed"),
                RefGenomeVersion.V38);
    }

    public void runFullPipelineAndCheckFinalStatus(final String inputMode, final PipelineStatus expectedStatus) throws Exception {
        runFullPipelineAndCheckFinalStatus(inputMode, expectedStatus, Optional.empty(), RefGenomeVersion.V37);
    }

    public void runFullPipelineAndCheckFinalStatus(final String inputMode, final PipelineStatus expectedStatus,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<String> targetedRegionsBed,
            final RefGenomeVersion refGenomeVersion) throws Exception {
        final PipelineMain victim = new PipelineMain();
        final String version = version();
        final String setName = noDots(inputMode + "-" + version);
        final String fixtureDir = "smoke_test/" + inputMode + "/";
        final String randomRunId = noDots(RandomStringUtils.random(5, true, false));
        final ImmutableArguments.Builder builder = Arguments.defaultsBuilder(Arguments.DefaultsProfile.DEVELOPMENT.toString())
                .sampleJson(Resources.testResource(fixtureDir + "samples.json"))
                .cloudSdkPath(findCloudSdk())
                .setId(setName)
                .runId(randomRunId)
                .runGermlineCaller(false)
                .cleanup(true)
                .outputBucket("smoketest-pipeline-output-pilot-1")
                .context(Context.DIAGNOSTIC)
                .targetRegionsBedLocation(targetedRegionsBed)
                .refGenomeVersion(refGenomeVersion);

        if (whoami.equals("root")) {
            String privateKeyPath = workingDir() + "/google-key.json";
            builder.privateKeyPath(privateKeyPath).uploadPrivateKeyPath(privateKeyPath);
        }

        Arguments arguments = builder.build();
        Storage storage = StorageProvider.from(arguments, CredentialProvider.from(arguments).get()).get();

        cleanupBucket(inputMode, arguments.outputBucket(), storage);

        PipelineState state = victim.start(arguments);
        assertThat(state.status()).isEqualTo(expectedStatus);

        File expectedFilesResource = new File(Resources.testResource(fixtureDir + "expected_output_files"));
        List<String> expectedFiles = FileUtils.readLines(expectedFilesResource, FILE_ENCODING);
        final String outputDir = setName + "-" + randomRunId;
        List<String> actualFiles = listOutput(outputDir, arguments.outputBucket(), storage);
        assertThat(actualFiles).containsOnlyElementsOf(expectedFiles);

        if (inputMode.equals("tumor-reference")) {
            ComparAssert.assertThat(storage, arguments.outputBucket(), outputDir)
                    .isEqualToTruthset(Resources.testResource(fixtureDir + "/truthset"))
                    .cleanup();
        }

        cleanupBucket(outputDir, arguments.outputBucket(), storage);
    }

    @NotNull
    private String version() {
        String version = System.getProperty("version");
        if (version.equals("local-SNAPSHOT")) {
            version = System.getProperty("user.name");
        }
        return version;
    }

    private List<String> listOutput(final String setName, final String archiveBucket, final Storage storage) {
        return archiveBlobs(setName, archiveBucket, storage).map(Blob::getName)
                .map(n -> n.replace(setName + "/", ""))
                .filter(n -> !n.equals(STAGED_FLAG_FILE))
                .collect(Collectors.toList());
    }

    @NotNull
    private Stream<Blob> archiveBlobs(final String setName, final String archiveBucket, final Storage storage) {
        return StreamSupport.stream(storage.get(archiveBucket).list(Storage.BlobListOption.prefix(setName)).iterateAll().spliterator(),
                true);
    }

    private void cleanupBucket(final String setName, final String archiveBucket, final Storage storage) {
        archiveBlobs(setName, archiveBucket, storage).forEach(Blob::delete);
    }

    private static String workingDir() {
        return System.getProperty("user.dir");
    }

    private static String noDots(final String version) {
        return version.replace(".", "").toLowerCase();
    }

    private String findCloudSdk() {
        if (whoami.equals("root")) {
            return CLOUD_SDK_PATH;
        }
        try {
            Process process = Runtime.getRuntime().exec(new String[] { "/usr/bin/which", "gcloud" });
            if (process.waitFor() == 0) {
                return new File(new String(process.getInputStream().readAllBytes())).getParent();
            }
        } catch (Exception e) {
            // Fall through to using the default
        }
        return String.format("/Users/%s/google-cloud-sdk/bin", whoami);
    }

}
