package com.hartwig.bam.adam;

import com.hartwig.bam.BamCreationPipeline;
import com.hartwig.bam.HadoopStatusReporter;
import com.hartwig.io.DataLocation;
import com.hartwig.io.FinalDataLocation;
import com.hartwig.patient.ReferenceGenome;

import org.apache.hadoop.fs.FileSystem;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;

public class Pipelines {

    public static BamCreationPipeline bamCreationConsolidated(final ADAMContext adamContext, final FileSystem fileSystem,
            final String workingDirectory, final String referenceGenomePath, final int bwaThreads, final boolean mergeFinalFile) {
        JavaADAMContext javaADAMContext = new JavaADAMContext(adamContext);
        ReferenceGenome referenceGenome = ReferenceGenome.of(fileSystem.getUri() + referenceGenomePath);
        DataLocation finalDataLocation = new FinalDataLocation(fileSystem, workingDirectory);
        return BamCreationPipeline.builder()
                .finalQC(new MultiplePrimaryAlignmentsQC())
                .alignment(new Bwa(referenceGenome, adamContext, fileSystem, bwaThreads))
                .finalDatasource(new HDFSAlignmentRDDSource(javaADAMContext, finalDataLocation))
                .finalBamStore(new HDFSBamStore(finalDataLocation, fileSystem, mergeFinalFile))
                .markDuplicates(new MarkDups())
                .statusReporter(new HadoopStatusReporter(fileSystem, workingDirectory))
                .build();
    }
}