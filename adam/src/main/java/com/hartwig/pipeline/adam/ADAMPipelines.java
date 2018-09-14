package com.hartwig.pipeline.adam;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.MoreExecutors;
import com.hartwig.io.DataLocation;
import com.hartwig.io.FinalDataLocation;
import com.hartwig.io.IntermediateDataLocation;
import com.hartwig.io.OutputType;
import com.hartwig.patient.KnownIndels;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.BamCreationPipeline;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;

import org.apache.hadoop.fs.FileSystem;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.jetbrains.annotations.NotNull;

public class ADAMPipelines {

    public static BamCreationPipeline bamCreation(final ADAMContext adamContext, final FileSystem fileSystem, final String workingDirectory,
            final String referenceGenomePath, final List<String> knownIndelPaths, final int bwaThreads, final boolean doQC,
            final boolean parallel, final boolean saveAsFile) {
        JavaADAMContext javaADAMContext = new JavaADAMContext(adamContext);
        ReferenceGenome referenceGenome = ReferenceGenome.of(fileSystem.getUri() + referenceGenomePath);
        IntermediateDataLocation intermediateDataLocation = new IntermediateDataLocation(fileSystem, workingDirectory);
        DataLocation finalDataLocation = new FinalDataLocation(fileSystem, workingDirectory);
        KnownIndels knownIndels =
                KnownIndels.of(knownIndelPaths.stream().map(path -> fileSystem.getUri() + path).collect(Collectors.toList()));
        return BamCreationPipeline.builder()
                .readCountQCFactory(ADAMReadCountCheck::from)
                .referenceFinalQC(ifEnabled(doQC,
                        ADAMFinalBAMQC.of(javaADAMContext, referenceGenome, CoverageThreshold.of(10, 90), CoverageThreshold.of(20, 70))))
                .tumorFinalQC(ifEnabled(doQC,
                        ADAMFinalBAMQC.of(javaADAMContext, referenceGenome, CoverageThreshold.of(30, 80), CoverageThreshold.of(60, 65))))
                .alignment(new ADAMBwa(referenceGenome, adamContext, fileSystem, bwaThreads))
                .alignmentDatasource(new HDFSAlignmentRDDSource(OutputType.ALIGNED, javaADAMContext, intermediateDataLocation))
                .finalDatasource(new HDFSAlignmentRDDSource(OutputType.INDEL_REALIGNED, javaADAMContext, intermediateDataLocation))
                .finalBamStore(new HDFSBamStore(finalDataLocation, fileSystem, true))
                .addBamEnrichment(new ADAMMarkDuplicatesAndSort(javaADAMContext, intermediateDataLocation))
                .addBamEnrichment(new ADAMRealignIndels(knownIndels, referenceGenome, javaADAMContext, intermediateDataLocation))
                .bamStore(new HDFSBamStore(intermediateDataLocation, fileSystem, saveAsFile))
                .executorService(parallel ? Executors.newFixedThreadPool(2) : MoreExecutors.sameThreadExecutor())
                .build();
    }

    @NotNull
    private static QualityControl<AlignmentRecordRDD> ifEnabled(final boolean doQC, final ADAMFinalBAMQC finalBAMQC) {
        return doQC ? finalBAMQC : alignments -> QCResult.ok();
    }
}
