package com.hartwig.pipeline.smoke;

import static java.lang.String.format;

import static com.hartwig.pipeline.testsupport.Assertions.assertThatOutput;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineMain;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.sbpapi.ObjectMappers;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpRun;
import com.hartwig.pipeline.sbpapi.SbpSet;
import com.hartwig.pipeline.storage.GSUtil;
import com.hartwig.pipeline.storage.Processes;
import com.hartwig.pipeline.storage.RCloneCloudCopy;
import com.hartwig.pipeline.testsupport.Resources;
import com.hartwig.pipeline.transfer.sbp.SbpFileTransfer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(value = IntegrationTest.class)
public class SmokeTest {
    private static final String GCP_REMOTE = "gs";
    private static final String S3_REMOTE = "s3";
    private static final String FILE_ENCODING = "UTF-8";
    private static final int SBP_SET_ID = 9;
    private static final int SBP_RUN_ID = 12;

    private static final String SET_ID = "CPCT12345678";
    private static final String REFERENCE_SAMPLE = SET_ID + "R";
    private static final String TUMOR_SAMPLE = SET_ID + "T";
    private File resultsDir;

    private String rclonePath;
    private String rclone;
    private String archiveBucket;
    private String archivePrivateKey;
    private String archiveProject;
    private String cloudSdkPath;

    @Before
    public void setUp() throws Exception {
        rclonePath = "/usr/bin";
        rclone = format("%s/rclone", rclonePath);
        archiveBucket = "hmf-output-test";
        archivePrivateKey = workingDir() + "/google-archive-key.json";
        archiveProject = "hmf-database";
        cloudSdkPath = "/usr/bin";

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

        Arguments arguments = Arguments.defaultsBuilder(Arguments.DefaultsProfile.DEVELOPMENT.toString())
                .privateKeyPath(workingDir() + "/google-key.json")
                .sampleDirectory(workingDir() + "/../samples")
                .version(version).cloudSdkPath(cloudSdkPath)
                .setId(SET_ID)
                .mode(Arguments.Mode.FULL)
                .runId(runId)
                .runGermlineCaller(false)
                .sbpApiRunId(SBP_RUN_ID)
                .sbpApiUrl(apiUrl)
                .rclonePath(rclonePath)
                .rcloneS3RemoteDownload(S3_REMOTE)
                .rcloneS3RemoteUpload(S3_REMOTE)
                .sbpS3Url("s3.us-east-1.amazonaws.com")
                .rcloneGcpRemote(GCP_REMOTE)
                .upload(true)
                .cleanup(true).archiveBucket(archiveBucket).archiveProject(archiveProject).archivePrivateKeyPath(archivePrivateKey)
                .build();

        SbpRestApi api = SbpRestApi.newInstance(arguments);

        String destinationBucket = bucketName(api);
        String setName = setName(api);

        rclone(delete(setName, GCP_REMOTE, arguments.patientReportBucket()));
        rclone(delete(setName, S3_REMOTE, destinationBucket));
        cleanupArchiveBucket(setName);

        PipelineState state = victim.start(arguments);
        assertThat(state.status()).isEqualTo(PipelineStatus.QC_FAILED);

        List<String> rcloneListing = listRemoteFilesNamesOnly(format("%s:%s/%s/", S3_REMOTE, destinationBucket, setName));
        File expectedFilesResource = new File(Resources.testResource("smoke_test/expected_output_files"));
        List<String> expectedFiles = FileUtils.readLines(expectedFilesResource, FILE_ENCODING);
        assertThat(rcloneListing).containsOnlyElementsOf(expectedFiles);

        RCloneCloudCopy rclone = new RCloneCloudCopy(rclonePath, GCP_REMOTE, S3_REMOTE, ProcessBuilder::new);
        assertThatAlignmentIsEqualToExpected(destinationBucket, setName, REFERENCE_SAMPLE, rclone);
        assertThatAlignmentIsEqualToExpected(destinationBucket, setName, TUMOR_SAMPLE, rclone);
    }

    private List<String> delete(final String setName, final String remote, final String bucket) {
        return ImmutableList.of("delete", format("%s:%s/%s", remote, bucket, setName));
    }

    private List<String> listRemoteFilesNamesOnly(String remoteParent) {
        return listRemoteFiles(remoteParent).stream().map(s -> s.split(" +")[1]).collect(Collectors.toList());
    }

    private List<String> listRemoteFiles(String remoteParent) {
        try {
            return rclone(ImmutableList.of("ls", remoteParent)).stream()
                    .map(String::trim)
                    .filter(s -> !s.split(" +")[1].equals(SbpFileTransfer.MANIFEST_FILENAME))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void cleanupArchiveBucket(String setName) {
        GSUtil.configure(true, 1);
        try {
            GSUtil.auth(cloudSdkPath, archivePrivateKey);
            runGsutil(ImmutableList.of("ls", format("gs://%s", archiveBucket)));
        } catch (Exception e) {
            throw new RuntimeException(format("Could not confirm archive bucket [%s] exists", archiveBucket));
        }
        try {
            runGsutil(ImmutableList.of("stat", format("gs://%s/%s", archiveBucket, setName)));
        } catch (Exception e) {
            // Folder does not exist, removal will fail so just return
            return;
        }
        runGsutil(ImmutableList.of("rm", "-r", format("gs://%s/%s", archiveBucket, setName)));
    }

    private void runGsutil(List<String> arguments) {
        try {
            ProcessBuilder process =
                    new ProcessBuilder(ImmutableList.<String>builder().add("gsutil", "-u", archiveProject).addAll(arguments).build());
            Processes.run(process, true, 1, TimeUnit.HOURS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> rclone(List<String> arguments) {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();
            List<String> command = ImmutableList.<String>builder().add(rclone).addAll(arguments).build();
            processBuilder.command(command);
            return IOUtils.readLines(processBuilder.start().getInputStream(), FILE_ENCODING);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assertThatAlignmentIsEqualToExpected(final String s3Bucket, final String setID, final String sample,
            final RCloneCloudCopy rclone) {
        String bam = sample + ".bam";
        File results = new File(resultsDir.getPath() + "/" + bam);

        rclone.copy(format("%s://%s/%s/%s/aligner/%s", S3_REMOTE, s3Bucket, setID, sample, bam), results.getPath());
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

    private static String bucketName(SbpRestApi api) throws IOException {
        return ObjectMappers.get().readValue(api.getRun(SBP_RUN_ID), SbpRun.class).bucket();
    }
}
