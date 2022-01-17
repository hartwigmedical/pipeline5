package com.hartwig.pipeline.smoke;

import static java.lang.String.format;

import static com.hartwig.pipeline.testsupport.Assertions.assertThatOutput;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.events.Pipeline.Context;
import com.hartwig.pipeline.Arguments;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(value = IntegrationTest.class)
public class SmokeTest {

    private static final String GCP_REMOTE = "gs";
    private static final String FILE_ENCODING = "UTF-8";
    private static final int SBP_RUN_ID = 102;
    private static final String SAMPLE_ID = "CPCT12345678";
    private static final String REFERENCE_SAMPLE = SAMPLE_ID + "R";
    private static final String TUMOR_SAMPLE = SAMPLE_ID + "T";
    private static final String STAGED_FLAG_FILE = "STAGED";
    private static final String RCLONE_PATH = "/usr/bin";
    private static final String CLOUD_SDK_PATH = "/root/google-cloud-sdk/bin";
    private File resultsDir;

    @Before
    public void setUp() throws Exception {
        resultsDir = new File(workingDir() + "/results");
        assertThat(resultsDir.mkdir()).isTrue();
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(resultsDir);
    }

    @Test
    public void runFullPipelineAndCheckFinalStatus() throws Exception {
        String apiUrl = "http://api.pilot-1";
        PipelineMain victim = new PipelineMain();
        String version = System.getProperty("version");
        String runId = "smoke-" + noDots(version);
        String setName = "smoke_test";

        String privateKeyPath = workingDir() + "/google-key.json";
        Arguments arguments = Arguments.defaultsBuilder(Arguments.DefaultsProfile.DEVELOPMENT.toString())
                .privateKeyPath(privateKeyPath)
                .uploadPrivateKeyPath(privateKeyPath)
                .cloudSdkPath(CLOUD_SDK_PATH)
                .setId(setName)
                .runId(runId)
                .runGermlineCaller(false)
                .sbpApiRunId(SBP_RUN_ID)
                .sbpApiUrl(apiUrl)
                .rclonePath(RCLONE_PATH)
                .rcloneGcpRemote(GCP_REMOTE)
                .rcloneS3RemoteDownload("s3")
                .cleanup(true)
                .outputBucket("services-pipeline-output-pilot-1")
                .context(Context.SERVICES)
                .build();
        Storage storage = StorageProvider.from(arguments, CredentialProvider.from(arguments).get()).get();

        cleanupBucket(setName, arguments.outputBucket(), storage);

        PipelineState state = victim.start(arguments);
        assertThat(state.status()).isEqualTo(PipelineStatus.QC_FAILED);

        File expectedFilesResource = new File(Resources.testResource("smoke_test/expected_output_files"));
        List<String> expectedFiles = FileUtils.readLines(expectedFilesResource, FILE_ENCODING);
        List<String> actualFiles = listOutput(setName, arguments.outputBucket(), storage);
        assertThat(actualFiles).containsOnlyElementsOf(expectedFiles);

        assertThatAlignmentIsEqualToExpected(setName, REFERENCE_SAMPLE, arguments.outputBucket(), storage);
        assertThatAlignmentIsEqualToExpected(setName, TUMOR_SAMPLE, arguments.outputBucket(), storage);

        cleanupBucket(setName, arguments.outputBucket(), storage);
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
