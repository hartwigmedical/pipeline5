package com.hartwig.pipeline.smoke;

import static com.hartwig.pipeline.testsupport.Assertions.assertThatOutput;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineMain;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(value = IntegrationTest.class)
public class SmokeTest {
    private static final String SET_ID = "CPCT12345678";
    private static final String REFERENCE_SAMPLE = SET_ID + "R";
    private static final String TUMOR_SAMPLE = SET_ID + "T";
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
        PipelineMain victim = new PipelineMain();
        String version = System.getProperty("version");
        String runId = "smoke-" + noDots(version);
        Arguments arguments = Arguments.defaultsBuilder(Arguments.DefaultsProfile.DEVELOPMENT.toString())
                .privateKeyPath("google-key.json")
                .sampleDirectory(workingDir() + "/../samples")
                .version(version)
                .jarDirectory(workingDir() + "/../bam/target")
                .cloudSdkPath("/usr/bin")
                .nodeInitializationScript(workingDir() + "/src/main/resources/node-init.sh")
                .setId(SET_ID)
                .mode(Arguments.Mode.FULL)
                .runId(runId)
                .runGermlineCaller(false)
                .build();
        PipelineState state = victim.start(arguments);
        assertThat(state.status()).isEqualTo(PipelineStatus.QC_FAILED);

        assertThatAlignmentIsEqualToExpected(runId, arguments, REFERENCE_SAMPLE);
        assertThatAlignmentIsEqualToExpected(runId, arguments, TUMOR_SAMPLE);
    }

    private void assertThatAlignmentIsEqualToExpected(final String runId, final Arguments arguments, final String sample) throws Exception {
        String bam = sample + ".bam";
        File results = new File(resultsDir.getPath() + "/" + bam);
        download(StorageOptions.getDefaultInstance().getService(),
                arguments.patientReportBucket(),
                SET_ID + "-" + runId + "/" + sample + "/aligner/" + bam,
                results);
        assertThatOutput(results.getParent(), "/" + sample).aligned().duplicatesMarked().sorted().isEqualToExpected();
    }

    private void download(Storage storage, String bucketName, String objectName, File downloadTo) throws Exception {
        BlobId blobId = BlobId.of(bucketName, objectName);
        Blob blob = storage.get(blobId);
        PrintStream writeTo = new PrintStream(new FileOutputStream(downloadTo));
        try (ReadChannel reader = blob.reader()) {
            WritableByteChannel channel = Channels.newChannel(writeTo);
            ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
            while (reader.read(bytes) > 0) {
                bytes.flip();
                channel.write(bytes);
                bytes.clear();
            }
        }
        writeTo.close();
    }

    private static String workingDir() {
        return System.getProperty("user.dir");
    }

    private static String noDots(final String version) {
        return version.replace(".", "");
    }
}