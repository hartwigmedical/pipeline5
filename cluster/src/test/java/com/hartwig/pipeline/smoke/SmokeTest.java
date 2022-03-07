package com.hartwig.pipeline.smoke;

import static java.lang.String.format;

import static com.hartwig.pipeline.testsupport.Assertions.assertThatOutput;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.in;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
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
import com.hartwig.pipeline.storage.StorageProvider;
import com.hartwig.pipeline.testsupport.Resources;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parallelized.class)
@Category(value = IntegrationTest.class)
public class SmokeTest {

    private static final String FILE_ENCODING = "UTF-8";
    private static final String SAMPLE_ID = "CPCT12345678";
    private static final String REFERENCE_SAMPLE = SAMPLE_ID + "R";
    private static final String TUMOR_SAMPLE = SAMPLE_ID + "T";
    private static final String STAGED_FLAG_FILE = "STAGED";
    private static final String CLOUD_SDK_PATH = "/root/google-cloud-sdk/bin";
    private File resultsDir;

    private final String inputMode;

    public SmokeTest(final String inputMode) {
        this.inputMode = inputMode;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { "tumor" } });
    }

    @Before
    public void setUp() throws Exception {
        resultsDir = new File(workingDir() + "/results");
        resultsDir.mkdir();
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(resultsDir);
    }

    @Test
    public void runFullPipelineAndCheckFinalStatus() throws Exception {
        PipelineMain victim = new PipelineMain();
        String version = version(inputMode);
        String setName = inputMode + "-" + version;
        final String fixtureDir = "smoke_test/" + inputMode + "/";
        final ImmutableArguments.Builder builder = Arguments.defaultsBuilder(Arguments.DefaultsProfile.DEVELOPMENT.toString())
                .sampleJson(Resources.testResource(fixtureDir + "samples.json"))
                .cloudSdkPath(CLOUD_SDK_PATH)
                .setId(inputMode)
                .runId(version)
                .runGermlineCaller(false)
                .cleanup(true)
                .outputBucket("smoketest-pipeline-output-pilot-1")
                .context(Context.DIAGNOSTIC);

        final String username = System.getProperty("user.name");
        if (username.equals("root")) {
            String privateKeyPath = workingDir() + "/google-key.json";
            builder.privateKeyPath(privateKeyPath).uploadPrivateKeyPath(privateKeyPath);
        } else {
            builder.cloudSdkPath(String.format("/Users/%s/google-cloud-sdk/bin", username));
        }

        Arguments arguments = builder.build();
        Storage storage = StorageProvider.from(arguments, CredentialProvider.from(arguments).get()).get();

        cleanupBucket(inputMode, arguments.outputBucket(), storage);

        PipelineState state = victim.start(arguments);
        assertThat(state.status()).isEqualTo(PipelineStatus.QC_FAILED);

        File expectedFilesResource = new File(Resources.testResource(fixtureDir + "expected_output_files"));
        List<String> expectedFiles = FileUtils.readLines(expectedFilesResource, FILE_ENCODING);
        List<String> actualFiles = listOutput(setName, arguments.outputBucket(), storage);
        assertThat(actualFiles).containsOnlyElementsOf(expectedFiles);
        cleanupBucket(setName, arguments.outputBucket(), storage);
    }

    @NotNull
    private String version(final String inputMode) {
        String version = System.getProperty("version");
        if (version.equals("local-SNAPSHOT")) {
            version = System.getProperty("user.name") + "-" + inputMode;
        }
        return version.length() > 14 ? version.substring(0, 14) : version;
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

    private void assertThatAlignmentIsEqualToExpected(final String setID, final String sample, final String archiveBucket,
            final Storage storage) throws IOException {
        String cram = sample + ".cram";
        File results = new File(resultsDir.getPath() + "/" + cram);
        Blob cramBlob = storage.get(archiveBucket).get(format("%s/%s/cram/%s", setID, sample, cram));
        FileOutputStream output = new FileOutputStream(results);
        output.write(cramBlob.getContent());
        output.close();
        assertThatOutput(results.getParent(), "/" + sample).aligned().duplicatesMarked().sorted().isEqualToExpected();
    }

    private static String workingDir() {
        return System.getProperty("user.dir");
    }

    private static String noDots(final String version) {
        return version.replace(".", "").toLowerCase();
    }
}
