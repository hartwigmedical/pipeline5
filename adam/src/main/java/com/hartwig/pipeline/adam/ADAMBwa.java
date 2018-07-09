package com.hartwig.pipeline.adam;

import static java.util.Collections.singletonList;

import static scala.collection.JavaConverters.asScalaBufferConverter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.exception.Exceptions;
import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Lane;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.api.java.AlignmentRecordsToAlignmentRecordsConverter;
import org.bdgenomics.adam.models.RecordGroup;
import org.bdgenomics.adam.models.RecordGroupDictionary;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.read.AnySAMOutFormatter;
import org.bdgenomics.adam.rdd.read.FASTQInFormatter;

import htsjdk.samtools.ValidationStringency;
import scala.Option;

class ADAMBwa implements Stage<Sample, AlignmentRecordRDD, AlignmentRecordRDD> {

    private final ADAMContext adamContext;
    private final ReferenceGenome referenceGenome;
    private final int bwaThreads;

    ADAMBwa(final ReferenceGenome referenceGenome, final ADAMContext adamContext, int bwaThreads) {
        this.adamContext = adamContext;
        this.referenceGenome = referenceGenome;
        this.bwaThreads = bwaThreads;
    }

    @Override
    public DataSource<Sample, AlignmentRecordRDD> datasource() {
        return entity -> null;
    }

    @Override
    public OutputType outputType() {
        return OutputType.ALIGNED;
    }

    @Override
    public InputOutput<Sample, AlignmentRecordRDD> execute(InputOutput<Sample, AlignmentRecordRDD> input) throws IOException {
        SequenceDictionary sequenceDictionary = adamContext.loadSequenceDictionary(referenceGenome.path() + ".dict");
        Sample sample = input.entity();
        List<AlignmentRecordRDD> laneRdds =
                sample.lanes().stream().map(lane -> adamBwa(sequenceDictionary, sample, lane)).collect(Collectors.toList());
        if (!laneRdds.isEmpty()) {
            return InputOutput.of(outputType(),
                    sample,
                    laneRdds.get(0).<AlignmentRecordRDD>union(asScalaBufferConverter(laneRdds.subList(1, laneRdds.size())).asScala()));
        }
        throw Exceptions.noLanesInSample();
    }

    private AlignmentRecordRDD adamBwa(final SequenceDictionary sequenceDictionary, final Sample sample, final Lane lane) {
        return RDDs.alignmentRecordRDD(adamContext.loadFastq(lane.matesPath(), Option.empty(), Option.empty(), ValidationStringency.LENIENT)
                .pipe(BwaCommand.tokens(referenceGenome, sample, lane, bwaThreads),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0,
                        FASTQInFormatter.class, new AnySAMOutFormatter(), new AlignmentRecordsToAlignmentRecordsConverter())
                .replaceRecordGroups(recordDictionary(recordGroup(sample, lane)))
                .replaceSequences(sequenceDictionary));
    }

    private RecordGroupDictionary recordDictionary(final RecordGroup recordGroup) {
        return new RecordGroupDictionary(asScalaBufferConverter(singletonList(recordGroup)).asScala());
    }

    private RecordGroup recordGroup(final Sample sample, final Lane lane) {
        return new RecordGroup(sample.name(),
                lane.recordGroupId(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.apply(sample.name()),
                Option.empty(),
                Option.apply("ILLUMINA"),
                Option.apply(lane.flowCellId()));
    }
}
