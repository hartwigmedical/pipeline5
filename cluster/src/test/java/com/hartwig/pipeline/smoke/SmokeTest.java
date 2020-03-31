package com.hartwig.pipeline.smoke;

import static java.lang.String.format;

import static com.hartwig.pipeline.testsupport.Assertions.assertThatOutput;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineMain;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpSet;
import com.hartwig.pipeline.storage.GSUtil;
import com.hartwig.pipeline.storage.GSUtilCloudCopy;
import com.hartwig.pipeline.testsupport.Resources;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(value = IntegrationTest.class)
public class SmokeTest {
    private static final String ARCHIVE_BUCKET = "hmf-output-test";
    private static final String GCP_REMOTE = "gs";
    private static final String FILE_ENCODING = "UTF-8";
    private static final int SBP_SET_ID = 9;
    private static final int SBP_RUN_ID = 12;

    private static final String SET_ID = "CPCT12345678";
    private static final String REFERENCE_SAMPLE = SET_ID + "R";
    private static final String TUMOR_SAMPLE = SET_ID + "T";
    private static final String STAGED_FLAG_FILE = "STAGED";
    private static final String RCLONE_PATH = "/usr/bin";
    private static final String ARCHIVE_PRIVATE_KEY = workingDir() + "/google-archive-key.json";
    private static final String ARCHIVE_PROJECT = "hmf-database";
    private static final String CLOUD_SDK_PATH = "/usr/bin";
    private File resultsDir;

    @Before
    public void setUp() throws Exception {

        System.setProperty("javax.net.ssl.keyStorePassword", "changeit");
        System.setProperty("javax.net.ssl.keyStore", Resources.testResource("smoke_test/api.jks"));

        resultsDir = new File(workingDir() + "/results");
        assertThat(resultsDir.mkdir()).isTrue();
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(resultsDir);
    }

    @Test
    public void runFullPipelineAndCheckFinalStatus() throws Exception {
        String apiUrl = "https://api.acc.hartwigmedicalfoundation.nl";
        PipelineMain victim = new PipelineMain();
        String version = System.getProperty("version");
        String runId = "smoke-" + noDots(version);
        GSUtil.configure(true, 1);

        String privateKeyPath = workingDir() + "/google-key.json";
        Arguments arguments = Arguments.defaultsBuilder(Arguments.DefaultsProfile.DEVELOPMENT.toString())
                .privateKeyPath(privateKeyPath)
                .uploadPrivateKeyPath(privateKeyPath)
                .sampleDirectory(workingDir() + "/../samples")
                .version(version)
                .cloudSdkPath(CLOUD_SDK_PATH)
                .setId(SET_ID)
                .runId(runId)
                .runGermlineCaller(false)
                .sbpApiRunId(SBP_RUN_ID)
                .sbpApiUrl(apiUrl)
                .rclonePath(RCLONE_PATH)
                .uploadFromGcp(true)
                .rcloneGcpRemote(GCP_REMOTE)
                .rcloneS3RemoteDownload("download")
                .upload(true)
                .cleanup(true)
                .archiveBucket(ARCHIVE_BUCKET)
                .archiveProject(ARCHIVE_PROJECT)
                .archivePrivateKeyPath(ARCHIVE_PRIVATE_KEY)
                .build();

        SbpRestApi api = SbpRestApi.newInstance(arguments.sbpApiUrl());

        String setName = setName(api);

        delete(format("gs://%s/%s", arguments.patientReportBucket(), setName));
        cleanupArchiveBucket(setName);

        PipelineState state = victim.start(arguments);
        assertThat(state.status()).isEqualTo(PipelineStatus.QC_FAILED);

        File expectedFilesResource = new File(Resources.testResource("smoke_test/expected_output_files"));
        List<String> expectedFiles = FileUtils.readLines(expectedFilesResource, FILE_ENCODING);
        List<String> archiveListing = listArchiveFilenames(setName);
        assertThat(archiveListing).containsOnlyElementsOf(expectedFiles);

        GSUtilCloudCopy gsutil = new GSUtilCloudCopy(arguments.cloudSdkPath());
        assertThatAlignmentIsEqualToExpected(setName, REFERENCE_SAMPLE, gsutil);
        assertThatAlignmentIsEqualToExpected(setName, TUMOR_SAMPLE, gsutil);
    }

    private List<String> listArchiveFilenames(String setName) {
        confirmArchiveBucketExists();
        String output = runGsUtil(ImmutableList.of("ls", "-r", format("gs://%s/%s", ARCHIVE_BUCKET, setName)));
        return ImmutableList.<String>builder().add(output.split("\n"))
                .build()
                .stream()
                .filter(filename -> filename.matches("^gs://.*[^:]"))
                .map(filename -> filename.replaceAll(format("^gs://%s/%s/", ARCHIVE_BUCKET, setName), ""))
                .filter(filename -> !filename.equals(STAGED_FLAG_FILE))
                .collect(Collectors.toList());
    }

    private void cleanupArchiveBucket(String setName) {
        confirmArchiveBucketExists();
        try {
            runGsUtil(ImmutableList.of("stat", format("gs://%s/%s", ARCHIVE_BUCKET, setName)));
        } catch (Exception e) {
            // Folder does not exist, removal will fail so just return
            return;
        }
        delete(format("gs://%s/%s", ARCHIVE_BUCKET, setName));
    }

    private void delete(final String path) {
        runGsUtil(ImmutableList.of("rm", "-r", path));
    }

    private String runGsUtil(List<String> arguments) {
        try {
            ProcessBuilder process =
                    new ProcessBuilder(ImmutableList.<String>builder().add("gsutil", "-u", ARCHIVE_PROJECT).addAll(arguments).build());
            return IOUtils.toString(process.start().getInputStream());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void confirmArchiveBucketExists() {
        try {
            GSUtil.auth(CLOUD_SDK_PATH, ARCHIVE_PRIVATE_KEY);
            runGsUtil(ImmutableList.of("ls", format("gs://%s", ARCHIVE_BUCKET)));
        } catch (Exception e) {
            throw new RuntimeException(format("Could not confirm archive bucket [%s] exists", ARCHIVE_BUCKET));
        }
    }

    private void assertThatAlignmentIsEqualToExpected(final String setID, final String sample, final GSUtilCloudCopy gsUtil) {
        String bam = sample + ".bam";
        File results = new File(resultsDir.getPath() + "/" + bam);

        runGsUtil(ImmutableList.of("cp",
                format("%s://%s/%s/%s/aligner/%s", GCP_REMOTE, ARCHIVE_BUCKET, setID, sample, bam),
                results.getPath()));
        assertThatOutput(results.getParent(), "/" + sample).aligned().duplicatesMarked().sorted().isEqualToExpected();
    }

    private static String workingDir() {
        return System.getProperty("user.dir");
    }

    private static String noDots(final String version) {
        return version.replace(".", "");
    }

    private static String setName(SbpRestApi api) throws IOException {
        List<SbpSet> sets = ObjectMappers.get().readValue(api.getSet(SBP_SET_ID), new TypeReference<List<SbpSet>>() {
        });
        return sets.get(0).name();
    }
}
