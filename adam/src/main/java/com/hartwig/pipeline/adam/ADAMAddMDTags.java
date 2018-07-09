package com.hartwig.pipeline.adam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.api.java.AlignmentRecordsToAlignmentRecordsConverter;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.read.AnySAMOutFormatter;
import org.bdgenomics.adam.rdd.read.BAMInFormatter;

class ADAMAddMDTags implements Stage<Sample, AlignmentRecordRDD, AlignmentRecordRDD> {

    private final JavaADAMContext javaADAMContext;
    private final ReferenceGenome referenceGenome;

    ADAMAddMDTags(final JavaADAMContext javaADAMContext, final ReferenceGenome referenceGenome) {
        this.javaADAMContext = javaADAMContext;
        this.referenceGenome = referenceGenome;
    }

    @Override
    public DataSource<Sample, AlignmentRecordRDD> datasource() {
        return new AlignmentRDDSource(OutputType.INDEL_REALIGNED, javaADAMContext);
    }

    @Override
    public OutputType outputType() {
        return OutputType.MD_TAGGED;
    }

    @Override
    public InputOutput<Sample, AlignmentRecordRDD> execute(final InputOutput<Sample, AlignmentRecordRDD> input) throws IOException {
        List<String> cmd = new ArrayList<>();
        cmd.add("samtools");
        cmd.add("calmd");
        cmd.add("-b");
        cmd.add("-");
        cmd.add(referenceGenome.path());
        return InputOutput.of(outputType(),
                input.entity(),
                RDDs.alignmentRecordRDD(input.payload()
                        .pipe(cmd,
                                Collections.emptyList(),
                                Collections.emptyMap(),
                                0,
                                BAMInFormatter.class,
                                new AnySAMOutFormatter(),
                                new AlignmentRecordsToAlignmentRecordsConverter())));
    }
}
