package com.hartwig.bam.before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.hartwig.bam.StatusReporter;
import com.hartwig.patient.ImmutableLane;
import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GunZip {

    private static final Logger LOGGER = LoggerFactory.getLogger(GunZip.class);
    private static final String GZ_EXTENSION = ".gz";
    private final FileSystem fileSystem;
    private final JavaSparkContext sparkContext;
    private final boolean alreadyUnzipped;
    private final StatusReporter statusReporter;

    GunZip(final FileSystem fileSystem, final JavaSparkContext sparkContext, final boolean alreadyUnzipped,
            final StatusReporter statusReporter) {
        this.fileSystem = fileSystem;
        this.sparkContext = sparkContext;
        this.alreadyUnzipped = alreadyUnzipped;
        this.statusReporter = statusReporter;
    }

    public Sample run(Sample sample) throws IOException {
        StatusReporter.Status status = StatusReporter.Status.SUCCESS;
        try {
            ImmutableSample.Builder builder = ImmutableSample.builder().from(sample);
            if (!alreadyUnzipped) {
                unzipAllParallel(sample);
            } else {
                onlyRenameFile(sample);
            }
            List<Lane> unzippedLanes = sample.lanes().parallelStream().map(lane -> {
                ImmutableLane.Builder laneBuilder = Lane.builder().from(lane);
                laneBuilder.firstOfPairPath(truncateGZExtension(lane.firstOfPairPath()));
                laneBuilder.secondOfPairPath(truncateGZExtension(lane.secondOfPairPath()));
                return laneBuilder.build();
            }).collect(Collectors.toList());
            return builder.lanes(unzippedLanes).build();
        } catch (Exception e) {
            status = StatusReporter.Status.FAILED_ERROR;
            throw e;
        } finally {
            statusReporter.report(status);
        }
    }

    private void onlyRenameFile(final Sample sample) throws IOException {
        for (Lane lane : sample.lanes()) {
            fileSystem.rename(new Path(lane.firstOfPairPath()), new Path(truncateGZExtension(lane.firstOfPairPath())));
            fileSystem.rename(new Path(lane.secondOfPairPath()), new Path(truncateGZExtension(lane.secondOfPairPath())));
        }
    }

    private void unzipAllParallel(final Sample sample) {
        if (!sample.lanes().isEmpty()) {
            ExecutorService executorService = Executors.newFixedThreadPool(sample.lanes().size() * 2);
            List<Future<?>> futures = new ArrayList<>();
            for (Lane lane : sample.lanes()) {
                unzipIfGZ(executorService, futures, lane.firstOfPairPath());
                unzipIfGZ(executorService, futures, lane.secondOfPairPath());
            }
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void unzipIfGZ(final ExecutorService executorService, final List<Future<?>> futures, final String path) {
        if (isGZ(path)) {
            futures.add(executorService.submit(() -> unzip(path)));
        }
    }

    private String truncateGZExtension(final String path) {
        return path.replaceAll(GZ_EXTENSION, "");
    }

    private static boolean isGZ(final String path) {
        return path.endsWith(GZ_EXTENSION);
    }

    private void unzip(final String path) {
        try {
            String unzippedPath = truncateGZExtension(path);
            sparkContext.textFile(path).saveAsTextFile(unzippedPath);
            fileSystem.delete(new Path(path), true);
            LOGGER.info("Unzipping [{}] complete. Deleting zipped file from HDFS", path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static Sample execute(FileSystem fileSystem, JavaSparkContext sparkContext, Sample sample, boolean alreadyUnzipped,
            StatusReporter statusReporter)
            throws IOException {
        GunZip gunZip = new GunZip(fileSystem, sparkContext, alreadyUnzipped, statusReporter);
        return gunZip.run(sample);
    }
}
