package com.hartwig.pipeline.after;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.pipeline.metrics.Monitor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.ivy.util.CopyProgressEvent;
import org.apache.ivy.util.CopyProgressListener;
import org.apache.ivy.util.FileUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BamMetricsPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(BamIndexPipeline.class);
    private final FileSystem fileSystem;
    private final String sourceBamDirectory;
    private final String sourceRefGenomeDirectory;
    private final String localWorkingDirectory;
    private final Monitor monitor;
    private final PicardWGSMetrics picardWGSMetrics;

    private BamMetricsPipeline(final FileSystem fileSystem, final String sourceBamDirectory, final String sourceRefGenomeDirectory,
            final String localWorkingDirectory, final Monitor monitor, final PicardWGSMetrics picardWGSMetrics) {
        this.fileSystem = fileSystem;
        this.sourceBamDirectory = sourceBamDirectory;
        this.sourceRefGenomeDirectory = sourceRefGenomeDirectory;
        this.localWorkingDirectory = localWorkingDirectory;
        this.monitor = monitor;
        this.picardWGSMetrics = picardWGSMetrics;
    }

    public void execute(Sample sample) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();

        String sourceBamFile = Bams.name(sample, sourceBamDirectory, Bams.SORTED);
        String localBamFile = localWorkingDirectory + File.separator + sample.name() + ".bam";

        LOGGER.info("Copying BAM file to [{}]", localBamFile);
        FileUtil.copy(fileSystem.open(new Path(sourceBamFile)), new File(localBamFile), noop());
        LOGGER.info("Copy complete");

        String localRefGenomeDirectory = localWorkingDirectory + File.separator + "refGenome";
        RemoteIterator<LocatedFileStatus> fileIterator = fileSystem.listFiles(new Path(sourceRefGenomeDirectory), false);
        String localRefGenomeFile = null;
        while (fileIterator.hasNext()) {
            LocatedFileStatus file = fileIterator.next();
            String localFilePath = localRefGenomeDirectory + File.separator + file.getPath().getName();

            LOGGER.info("Copying ref genome file [{}]", localFilePath);
            FileUtil.copy(fileSystem.open(file.getPath()), new File(localFilePath), noop());

            if (localFilePath.endsWith(".fasta") || localFilePath.endsWith(".fa")) {
                localRefGenomeFile = localFilePath;
            }
        }

        assert localRefGenomeFile != null;

        String localWgsMetricsFile = localWorkingDirectory + File.separator + sample.name() + ".local.wgsmetrics";
        picardWGSMetrics.execute(localWorkingDirectory, localBamFile, localRefGenomeFile, localWgsMetricsFile);

        FileUtil.copy(new FileInputStream(localWgsMetricsFile),
                fileSystem.create(new Path(sourceBamFile + ".wgsmetrics")),
                noop());

// TODO: Sort out clean up of working directory without affecting test
//        FileUtil.forceDelete(new File(localWgsMetricsFile));

        long endTime = System.currentTimeMillis();
        monitor.update(Metric.spentTime("BAM_METRICS", endTime - startTime));
    }

    @VisibleForTesting
    static BamMetricsPipeline create(final FileSystem fileSystem, final String bamDirectory, final String refGenomeDirectory,
            final String localWorkingDirectory, final String picardLibDirectory, final Monitor monitor) {
        return new BamMetricsPipeline(fileSystem,
                bamDirectory,
                refGenomeDirectory,
                localWorkingDirectory,
                monitor,
                new PicardWGSMetrics(picardLibDirectory));
    }

    public static BamMetricsPipeline create(final FileSystem fileSystem, final String bamDirectory, final String refGenomeDirectory,
            final String picardLibDirectory, final Monitor monitor) {
        // For production usage, the local working directory is transient so doesn't matter.
        return create(fileSystem, bamDirectory, refGenomeDirectory, System.getProperty("user.dir"), picardLibDirectory, monitor);
    }

    @NotNull
    private static CopyProgressListener noop() {
        return new CopyProgressListener() {
            @Override
            public void start(final CopyProgressEvent copyProgressEvent) {
            }

            @Override
            public void progress(final CopyProgressEvent copyProgressEvent) {
            }

            @Override
            public void end(final CopyProgressEvent copyProgressEvent) {
            }
        };
    }
}
