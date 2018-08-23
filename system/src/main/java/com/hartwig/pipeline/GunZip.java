package com.hartwig.pipeline;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.patient.ImmutableLane;
import com.hartwig.patient.ImmutablePatient;
import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GunZip {

    private static final Logger LOGGER = LoggerFactory.getLogger(GunZip.class);
    private final FileSystem fileSystem;
    private final JavaSparkContext sparkContext;

    GunZip(final FileSystem fileSystem, final JavaSparkContext sparkContext) {
        this.fileSystem = fileSystem;
        this.sparkContext = sparkContext;
    }

    public Sample run(Sample sample) {
        ImmutableSample.Builder builder = ImmutableSample.builder().from(sample);
        List<Lane> unzippedLanes = sample.lanes().parallelStream().map(lane -> {
            try {
                ImmutableLane.Builder laneBuilder = Lane.builder().from(lane);
                laneBuilder.readsPath(unzip(lane.readsPath()));
                laneBuilder.matesPath(unzip(lane.matesPath()));
                return laneBuilder.build();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        return builder.lanes(unzippedLanes).build();
    }

    private String unzip(final String path) throws IOException {
        if (path.endsWith(".gz")) {
            LOGGER.info("Patient has a zipped input file [{}]. Unzipping now...", path);
            String unzippedPath = path.substring(0, path.length() - 3);
            sparkContext.textFile(path).saveAsTextFile(unzippedPath);
            fileSystem.delete(new Path(path), true);
            LOGGER.info("Unzipping [{}] complete. Deleting zipped file from HDFS", path);
            return unzippedPath;
        }
        return path;
    }

    public static Patient execute(FileSystem fileSystem, JavaSparkContext sparkContext, Patient patient) {
        GunZip gunZip = new GunZip(fileSystem, sparkContext);
        ImmutablePatient.Builder unzippedBuilder = ImmutablePatient.builder().from(patient);
        unzippedBuilder.reference(gunZip.run(patient.reference()));
        if (patient.maybeTumor().isPresent()) {
            unzippedBuilder.maybeTumor(gunZip.run(patient.tumor()));
        }
        return unzippedBuilder.build();
    }
}
