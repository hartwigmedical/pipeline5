package com.hartwig.pipeline.adam;

import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.io.DataLocation;
import com.hartwig.io.FinalDataLocation;
import com.hartwig.patient.KnownIndels;
import com.hartwig.patient.KnownSnps;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.BamCreationPipeline;
import com.hartwig.pipeline.HadoopStatusReporter;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;
import com.hartwig.pipeline.metrics.Monitor;

import org.apache.hadoop.fs.FileSystem;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.jetbrains.annotations.NotNull;

public class Pipelines {

    public static BamCreationPipeline bamCreationConsolidated(final ADAMContext adamContext, final FileSystem fileSystem,
            final String workingDirectory, final String referenceGenomePath, final List<String> knownIndelPaths,
            final List<String> knownSnpPaths, final int bwaThreads, final boolean mergeFinalFile) {
        JavaADAMContext javaADAMContext = new JavaADAMContext(adamContext);
        ReferenceGenome referenceGenome = ReferenceGenome.of(fileSystem.getUri() + referenceGenomePath);
        DataLocation finalDataLocation = new FinalDataLocation(fileSystem, workingDirectory);
        KnownIndels knownIndels = KnownIndels.of(fsPaths(fileSystem, knownIndelPaths));
        KnownSnps knownSnps = KnownSnps.of(fsPaths(fileSystem, knownSnpPaths));
        return BamCreationPipeline.builder()
                .finalQC(new MultiplePrimaryAlignmentsQC())
                .alignment(new Bwa(referenceGenome, adamContext, fileSystem, bwaThreads))
                .finalDatasource(new HDFSAlignmentRDDSource(javaADAMContext, finalDataLocation))
                .finalBamStore(new HDFSBamStore(finalDataLocation, fileSystem, mergeFinalFile))
                .markDuplicates(new MarkDups())
                .recalibration(new BaseQualityScoreRecalibration(knownSnps, knownIndels, javaADAMContext))
                .statusReporter(new HadoopStatusReporter(fileSystem, workingDirectory))
                .build();
    }

    private static List<String> fsPaths(final FileSystem fileSystem, final List<String> paths) {
        return paths.stream().map(path -> fileSystem.getUri() + path).collect(Collectors.toList());
    }
}