package com.hartwig.bam;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.support.hadoop.Hadoop;
import com.hartwig.support.test.Resources;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HadoopStatusReporterTest {

    private FileSystem fileSystem;
    private String reportDirectory;
    private String name;
    private StatusReporter victim;

    @Before
    public void setUp() throws Exception {
        fileSystem = Hadoop.localFilesystem();
        reportDirectory = Resources.targetResource("status_reporter_test/");
        fileSystem.delete(new Path(reportDirectory), true);
        name = "job_name";
        victim = new HadoopStatusReporter(fileSystem, reportDirectory, name);
    }

    @Test
    public void writesSuccessFileWhenStatusSuccess() throws Exception {
        victim.report(StatusReporter.Status.SUCCESS);
        assertThat(fileSystem.exists(new Path(reportDirectory + name + StatusReporter.SUCCESS))).isTrue();
    }

    @Test
    public void writesFailureFileWhenStatusFailed() throws Exception {
        victim.report(StatusReporter.Status.FAILED_READ_COUNT);
        assertThat(fileSystem.exists(new Path(reportDirectory + name + StatusReporter.FAILURE))).isTrue();
    }
}