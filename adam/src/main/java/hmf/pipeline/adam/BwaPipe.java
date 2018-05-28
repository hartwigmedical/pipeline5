package hmf.pipeline.adam;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import static scala.collection.JavaConverters.asScalaBufferConverter;

import java.io.IOException;
import java.util.Collections;

import org.bdgenomics.adam.api.java.FragmentsToAlignmentRecordsConverter;
import org.bdgenomics.adam.models.RecordGroup;
import org.bdgenomics.adam.models.RecordGroupDictionary;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.fragment.FragmentRDD;
import org.bdgenomics.adam.rdd.fragment.InterleavedFASTQInFormatter;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.read.AnySAMOutFormatter;

import hmf.io.PipelineOutput;
import hmf.pipeline.Stage;
import hmf.sample.Lane;
import hmf.sample.Reference;
import scala.Option;

class BwaPipe implements Stage<Lane> {

    private final ADAMContext adamContext;
    private final Reference reference;

    BwaPipe(final Reference reference, final ADAMContext adamContext) {
        this.adamContext = adamContext;
        this.reference = reference;
    }

    @Override
    public PipelineOutput output() {
        return PipelineOutput.ALIGNED;
    }

    @Override
    public void execute(Lane lane) throws IOException {
        FragmentRDD fragmentRDD = adamContext.loadInterleavedFastqAsFragments(format("%s/%s_L00%s_interleaved.fastq",
                lane.sample().directory(),
                lane.sample().name(),
                lane.index()));
        AlignmentRecordRDD aligned = fragmentRDD.pipe(BwaCommand.tokens(reference, lane.sample()),
                Collections.emptyList(),
                Collections.emptyMap(),
                0,
                InterleavedFASTQInFormatter.class,
                new AnySAMOutFormatter(),
                new FragmentsToAlignmentRecordsConverter());
        SequenceDictionary sequenceDictionary = adamContext.loadSequenceDictionary(reference.path() + ".dict");
        aligned.replaceRecordGroups(recordDictionary(recordGroup(lane.sample().name())))
                .replaceSequences(sequenceDictionary)
                .save(Persistence.defaultSave(lane, output()), false);
    }

    private RecordGroupDictionary recordDictionary(final RecordGroup recordGroup) {
        return new RecordGroupDictionary(asScalaBufferConverter(singletonList(recordGroup)).asScala());
    }

    private RecordGroup recordGroup(final String sample) {
        return new RecordGroup(sample,
                sample,
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.empty(), Option.empty(), Option.apply(sample),
                Option.empty(),
                Option.empty(),
                Option.empty());
    }
}
