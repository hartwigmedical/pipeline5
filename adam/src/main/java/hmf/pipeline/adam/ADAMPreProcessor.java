package hmf.pipeline.adam;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import static scala.collection.JavaConverters.asScalaBufferConverter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.bdgenomics.adam.api.java.AlignmentRecordsToAlignmentRecordsConverter;
import org.bdgenomics.adam.models.RecordGroup;
import org.bdgenomics.adam.models.RecordGroupDictionary;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.read.AnySAMOutFormatter;
import org.bdgenomics.adam.rdd.read.FASTQInFormatter;

import hmf.exception.Exceptions;
import hmf.io.Output;
import hmf.io.OutputType;
import hmf.patient.Lane;
import hmf.patient.Reference;
import hmf.patient.Sample;
import hmf.pipeline.Stage;
import htsjdk.samtools.ValidationStringency;
import scala.Option;

class ADAMPreProcessor implements Stage<Sample, AlignmentRecordRDD> {

    private final ADAMContext adamContext;
    private final Reference reference;

    ADAMPreProcessor(final Reference reference, final ADAMContext adamContext) {
        this.adamContext = adamContext;
        this.reference = reference;
    }

    @Override
    public Output<Sample, AlignmentRecordRDD> execute(Sample sample) throws IOException {
        SequenceDictionary sequenceDictionary = adamContext.loadSequenceDictionary(reference.path() + ".dict");
        List<AlignmentRecordRDD> laneRdds =
                sample.lanes().stream().map(lane -> adamBwa(sequenceDictionary, sample, lane)).collect(Collectors.toList());
        if (!laneRdds.isEmpty()) {
            return Output.of(OutputType.DUPLICATE_MARKED,
                    sample,
                    laneRdds.get(0).<AlignmentRecordRDD>union(asScalaBufferConverter(laneRdds.subList(1,
                            laneRdds.size())).asScala()).markDuplicates());
        }
        throw Exceptions.noLanesInSample();
    }

    private AlignmentRecordRDD adamBwa(final SequenceDictionary sequenceDictionary, final Sample sample, final Lane lane) {
        return adamContext.loadPairedFastq(format("%s/%s_L00%s_R1.fastq", lane.directory(), sample.name(), lane.index()),
                format("%s/%s_L00%s_R2.fastq", lane.directory(), sample.name(), lane.index()),
                Option.empty(),
                ValidationStringency.DEFAULT_STRINGENCY)
                .pipe(BwaCommand.tokens(reference, sample),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0, FASTQInFormatter.class, new AnySAMOutFormatter(), new AlignmentRecordsToAlignmentRecordsConverter())
                .replaceRecordGroups(recordDictionary(recordGroup(sample.name())))
                .replaceSequences(sequenceDictionary);
    }

    private RecordGroupDictionary recordDictionary(final RecordGroup recordGroup) {
        return new RecordGroupDictionary(asScalaBufferConverter(singletonList(recordGroup)).asScala());
    }

    private RecordGroup recordGroup(final String sample) {
        return new RecordGroup(sample,
                sample, Option.empty(), Option.empty(), Option.empty(), Option.empty(), Option.empty(), Option.apply(sample),
                Option.empty(),
                Option.empty(),
                Option.empty());
    }
}
