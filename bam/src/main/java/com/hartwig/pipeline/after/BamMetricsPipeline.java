package com.hartwig.pipeline.after;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.pipeline.metrics.Monitor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
    private final String localBamDirectory;
    private final Monitor monitor;
    private final PicardWGSMetrics picardWGSMetrics;

    private BamMetricsPipeline(final FileSystem fileSystem, final String sourceBamDirectory, final String localBamDirectory,
            final Monitor monitor, final PicardWGSMetrics picardWGSMetrics) {
        this.fileSystem = fileSystem;
        this.sourceBamDirectory = sourceBamDirectory;
        this.localBamDirectory = localBamDirectory;
        this.monitor = monitor;
        this.picardWGSMetrics = picardWGSMetrics;
    }

    public void execute(Sample sample) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        String bamFileLocation = String.format("/%s/%s.bam", sourceBamDirectory, sample.name()).substring(1);
        String unsortedBam = Bams.name(sample, localBamDirectory, "unsorted");

        LOGGER.info("Copying BAM file to [{}]", unsortedBam);
        FileUtil.copy(fileSystem.open(new Path(bamFileLocation)), new File(unsortedBam), noop());
        LOGGER.info("Copy complete");

        picardWGSMetrics.execute(sample, localBamDirectory);

        String sortedBam = Bams.name(sample, localBamDirectory, Bams.SORTED);
        String bai = Bams.bai(sample, localBamDirectory, Bams.SORTED);
        FileUtil.copy(new FileInputStream(sortedBam),
                fileSystem.create(new Path(Bams.name(sample, sourceBamDirectory, Bams.SORTED))),
                noop());
        FileUtil.copy(new FileInputStream(bai),
                fileSystem.create(new Path(Bams.name(sample, sourceBamDirectory, Bams.SORTED) + ".bai")),
                noop());

        long endTime = System.currentTimeMillis();
        monitor.update(Metric.spentTime("SORT_AND_INDEX", endTime - startTime));
    }

    public static BamMetricsPipeline fallback(final FileSystem fileSystem, final String sourceFolder, final Monitor monitor) {
        return new BamMetricsPipeline(fileSystem, sourceFolder, System.getProperty("user.dir"), monitor, new PicardWGSMetrics());
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
