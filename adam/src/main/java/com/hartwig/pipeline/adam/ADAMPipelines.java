package com.hartwig.pipeline.adam;

import java.util.List;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.MoreExecutors;
import com.hartwig.io.OutputType;
import com.hartwig.patient.KnownIndels;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.BamCreationPipeline;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.jetbrains.annotations.NotNull;

public class ADAMPipelines {

    public static BamCreationPipeline bamCreation(final String referenceGenomePath, final List<String> knownIndelPaths,
            final ADAMContext adamContext, final int bwaThreads, final boolean doQC, final boolean parallel) {
        JavaADAMContext javaADAMContext = new JavaADAMContext(adamContext);
        ReferenceGenome referenceGenome = ReferenceGenome.of(referenceGenomePath);
        return BamCreationPipeline.builder()
                .readCountQCFactory(ADAMReadCountCheck::from)
                .referenceFinalQC(ifEnabled(doQC,
                        ADAMFinalBAMQC.of(javaADAMContext, referenceGenome, CoverageThreshold.of(10, 90), CoverageThreshold.of(20, 70))))
                .tumourFinalQC(ifEnabled(doQC,
                        ADAMFinalBAMQC.of(javaADAMContext, referenceGenome, CoverageThreshold.of(30, 80), CoverageThreshold.of(60, 65))))
                .alignment(new ADAMBwa(referenceGenome, adamContext, bwaThreads))
                .alignmentDatasource(new AlignmentRDDSource(OutputType.ALIGNED, javaADAMContext))
                .finalDatasource(new AlignmentRDDSource(OutputType.INDEL_REALIGNED, javaADAMContext))
                .addBamEnrichment(new ADAMMarkDuplicatesAndSort(javaADAMContext))
                .addBamEnrichment(new ADAMRealignIndels(KnownIndels.of(knownIndelPaths), referenceGenome, javaADAMContext))
                .bamStore(new ADAMBAMStore())
                .executorService(parallel ? Executors.newFixedThreadPool(2) : MoreExecutors.sameThreadExecutor())
                .build();
    }

    @NotNull
    private static QualityControl<AlignmentRecordRDD> ifEnabled(final boolean doQC, final ADAMFinalBAMQC finalBAMQC) {
        return doQC ? finalBAMQC : alignments -> QCResult.ok();
    }
}
