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

public class BamIndexPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(BamIndexPipeline.class);
    private final FileSystem fileSystem;
    private final String sourceBamDirectory;
    private final String localBamDirectory;
    private final Monitor monitor;
    private final SortAndIndexer sortAndIndexer;

    private BamIndexPipeline(final FileSystem fileSystem, final String bamFolder, final String localBamLocation, final Monitor monitor,
            final SortAndIndexer sortAndIndexer) {
        this.fileSystem = fileSystem;
        this.sourceBamDirectory = bamFolder;
        this.localBamDirectory = localBamLocation;
        this.monitor = monitor;
        this.sortAndIndexer = sortAndIndexer;
    }

    public void execute(Sample sample) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        String bamFileLocation = String.format("/%s/%s.bam", sourceBamDirectory, sample.name()).substring(1);
        String unsortedBam = Bams.name(sample, localBamDirectory, "unsorted");

        LOGGER.info("Copying BAM file to [{}]", unsortedBam);
        FileUtil.copy(fileSystem.open(new Path(bamFileLocation)), new File(unsortedBam), noop());
        LOGGER.info("Copy complete");

        sortAndIndexer.execute(sample, localBamDirectory);

        String sortedBam = Bams.name(sample, localBamDirectory, Bams.SORTED);
        FileUtil.copy(new FileInputStream(sortedBam),
                fileSystem.create(new Path(Bams.name(sample, sourceBamDirectory, Bams.SORTED))),
                noop());
        FileUtil.copy(new FileInputStream(sortedBam),
                fileSystem.create(new Path(Bams.name(sample, sourceBamDirectory, Bams.SORTED) + ".bai")),
                noop());

        long endTime = System.currentTimeMillis();
        monitor.update(Metric.spentTime("SORT_AND_INDEX", endTime - startTime));
    }

    public static BamIndexPipeline fallback(final FileSystem fileSystem, final String sourceFolder, final Monitor monitor) {
        return fallback(fileSystem, sourceFolder, System.getProperty("user.dir"), monitor);
    }

    public static BamIndexPipeline fallback(final FileSystem fileSystem, final String sourceFolder, final String targetFolder,
            final Monitor monitor) {
        return new BamIndexPipeline(fileSystem, sourceFolder, targetFolder, monitor, (sample, sourceBam) -> {
            try {
                new SambambaSortAndIndex().execute(sample, sourceBam);
            } catch (Exception e) {
                LOGGER.warn("Unable to run sambamba sort and index, falling back on samtools");
                new SamtoolsSortAndIndex().execute(sample, sourceBam);
            }
        });
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
