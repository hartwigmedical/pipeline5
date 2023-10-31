package com.hartwig.pipeline.smoke;

import static java.lang.String.format;

import static com.hartwig.pipeline.resource.RefGenomeVersion.V37;
import static com.hartwig.pipeline.tools.VersionUtils.imageVersion;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.pdl.PdlJsonConversion;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ImmutableArguments;
import com.hartwig.pipeline.PipelineMain;
import com.hartwig.pipeline.PipelineState;
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

    protected static final String FILE_ENCODING = "UTF-8";
    protected static final String STAGED_FLAG_FILE = "STAGED";
    protected static final String CLOUD_SDK_PATH = "/root/google-cloud-sdk/bin";
    protected static final String INPUT_MODE_TUMOR_REF = "tumor-reference";
    protected static final String INPUT_MODE_TUMOR_ONLY = "tumor";
    protected static final String INPUT_MODE_REF_ONLY = "reference";

    private File resultsDir;
    private String whoami;

    protected static List<String> listOutput(final String setName, final String archiveBucket, final Storage storage) {
        return archiveBlobs(setName, archiveBucket, storage).map(Blob::getName)
                .map(n -> n.replace(setName + "/", ""))
                .filter(n -> !n.equals(STAGED_FLAG_FILE))
                .collect(Collectors.toList());
    }

    @NotNull
    protected static Stream<Blob> archiveBlobs(final String setName, final String archiveBucket, final Storage storage) {
        return StreamSupport.stream(storage.get(archiveBucket).list(Storage.BlobListOption.prefix(setName)).iterateAll().spliterator(),
                true);
    }

    private static String workingDir() {
        return System.getProperty("user.dir");
    }

    private static String noDots(final String version) {
        return version.replace(".", "").toLowerCase();
    }

    protected static String findCloudSdk(final String whoami) {
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
        return format("/Users/%s/google-cloud-sdk/bin", whoami);
    }

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
        runFullPipelineAndCheckFinalStatus(INPUT_MODE_TUMOR_REF, PipelineStatus.QC_FAILED);
    }

    @Test
    public void tumorOnly() throws Exception {
        runFullPipelineAndCheckFinalStatus(INPUT_MODE_TUMOR_ONLY, PipelineStatus.QC_FAILED);
    }

    @Test
    public void referenceOnly() throws Exception {
        runFullPipelineAndCheckFinalStatus(INPUT_MODE_REF_ONLY, PipelineStatus.QC_FAILED);
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
        runFullPipelineAndCheckFinalStatus(inputMode, expectedStatus, Optional.empty(), V37);
    }

    public void runFullPipelineAndCheckFinalStatus(final String inputMode, final PipelineStatus expectedStatus,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<String> targetedRegionsBed,
            final RefGenomeVersion refGenomeVersion) throws Exception {
        PipelineMain victim = new PipelineMain();
        String fixtureDir = "smoke_test/" + inputMode + "/";
        @SuppressWarnings("deprecation")
        String randomRunId = noDots(RandomStringUtils.random(3, true, false));
        String inputModeId = inputMode.equals(INPUT_MODE_TUMOR_REF) ? "tr" : (inputMode.equals(INPUT_MODE_TUMOR_ONLY) ? "t" : "r");
        String runTag = format("st_%s_%s_%s", imageVersion(), inputModeId, randomRunId);
        String sampleJson = Resources.testResource(fixtureDir + "samples.json");
        PipelineInput pipelineInput = PdlJsonConversion.getInstance().read(sampleJson);
        String setName = pipelineInput.setName() + "-" + runTag;
        ImmutableArguments.Builder builder = Arguments.defaultsBuilder(Arguments.DefaultsProfile.DEVELOPMENT.toString())
                .cleanup(false)
                .context(Pipeline.Context.PLATINUM)
                .sampleJson(sampleJson)
                .cloudSdkPath(findCloudSdk())
                .runTag(runTag)
                .runGermlineCaller(false)
                .outputBucket("smoketest-pipeline-output-pilot-1")
                .useTargetRegions(false)
                .refGenomeVersion(refGenomeVersion);
        //                .serviceAccountEmail("pipeline5-build@hmf-build.iam.gserviceaccount.com");

        Arguments arguments = builder.build();
        Storage storage = StorageProvider.from(arguments, GoogleCredentials.getApplicationDefault()).get();

        cleanupBucket(setName, arguments.outputBucket(), storage);

        PipelineState state = victim.start(arguments);
        assertThat(state.status()).isEqualTo(expectedStatus);

        File expectedFilesResource = new File(Resources.testResource(fixtureDir + "expected_output_files"));
        List<String> expectedFiles = FileUtils.readLines(expectedFilesResource, FILE_ENCODING);
        List<String> actualFiles = listOutput(setName, arguments.outputBucket(), storage);
        assertThat(actualFiles).containsOnlyElementsOf(expectedFiles);

        if (inputMode.equals(INPUT_MODE_TUMOR_REF)) {
            ComparAssert.assertThat(storage, arguments.outputBucket(), setName)
                    .isEqualToTruthset(Resources.testResource(fixtureDir + "/truthset"))
                    .cleanup();
        }

        cleanupBucket(setName, arguments.outputBucket(), storage);
    }

    private void cleanupBucket(final String setName, final String archiveBucket, final Storage storage) {
        archiveBlobs(setName, archiveBucket, storage).forEach(Blob::delete);
    }

    private String findCloudSdk() {
        return findCloudSdk(whoami);
    }
}
