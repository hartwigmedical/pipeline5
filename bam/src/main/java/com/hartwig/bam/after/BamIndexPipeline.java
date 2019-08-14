package com.hartwig.bam.after;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.hartwig.bam.HadoopStatusReporter;
import com.hartwig.bam.StatusReporter;
import com.hartwig.patient.Sample;

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
    private final SortAndIndexer sortAndIndexer;

    private BamIndexPipeline(final FileSystem fileSystem, final String bamFolder, final String localBamLocation,
            final SortAndIndexer sortAndIndexer) {
        this.fileSystem = fileSystem;
        this.sourceBamDirectory = bamFolder;
        this.localBamDirectory = localBamLocation;
        this.sortAndIndexer = sortAndIndexer;
    }

    public void execute(Sample sample, StatusReporter statusReporter) {
        StatusReporter.Status status = StatusReporter.Status.SUCCESS;
        try {
            String bamFileLocation = String.format("/%s/%s.bam", sourceBamDirectory, sample.name()).substring(1);
            String unsortedBam = Bams.name(sample, localBamDirectory, "unsorted");

            LOGGER.info("Copying BAM file to [{}]", unsortedBam);
            FileUtil.copy(fileSystem.open(new Path(bamFileLocation)), new File(unsortedBam), noop());
            LOGGER.info("Copy complete");

            sortAndIndexer.execute(sample, localBamDirectory);

            String sortedBam = Bams.name(sample, localBamDirectory, Bams.SORTED);
            String bai = Bams.bai(sample, localBamDirectory, Bams.SORTED);
            FileUtil.copy(new FileInputStream(sortedBam),
                    fileSystem.create(new Path(Bams.name(sample, sourceBamDirectory, Bams.SORTED))),
                    noop());
            FileUtil.copy(new FileInputStream(bai),
                    fileSystem.create(new Path(Bams.name(sample, sourceBamDirectory, Bams.SORTED) + ".bai")),
                    noop());
        } catch (Exception e) {
            status = StatusReporter.Status.FAILED_ERROR;
        } finally {
            statusReporter.report(status);
        }
    }

    public static BamIndexPipeline fallback(final FileSystem fileSystem, final String sourceFolder) {
        return fallback(fileSystem, sourceFolder, System.getProperty("user.dir"));
    }

    public static BamIndexPipeline fallback(final FileSystem fileSystem, final String sourceFolder, final String targetFolder) {
        return new BamIndexPipeline(fileSystem, sourceFolder, targetFolder, (sample, sourceBam) -> {
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
