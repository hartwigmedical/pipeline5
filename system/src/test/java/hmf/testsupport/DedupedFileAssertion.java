package hmf.testsupport;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import hmf.pipeline.PipelineOutput;
import hmf.sample.FlowCell;
import htsjdk.samtools.SAMFlag;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

public class DedupedFileAssertion {
    private final FlowCell cell;

    DedupedFileAssertion(final FlowCell cell) {
        this.cell = cell;
    }

    public void isEqualToExpected() throws IOException {

        InputStream expected = Assertions.class.getResourceAsStream(format("/expected/%s", PipelineOutput.DEDUPED.file(cell)));
        if (expected == null) {
            fail(format("No expected file found for sample [%s] and output [%s]. Check that the sample name is correct and there is a "
                    + "file in /src/test/resources/expected to verify against", cell.sample().name(), PipelineOutput.DEDUPED));
        }

        SamReaderFactory samReaderFactory = SamReaderFactory.make();
        SamReader samReaderExpected = samReaderFactory.open(SamInputResource.of(expected));
        SamReader samReaderResults = samReaderFactory.open(new File(PipelineOutput.DEDUPED.path(cell)));
        Set<SAMRecord> duplicatesExpected = findDuplicates(samReaderExpected);
        Set<SAMRecord> duplicatesResults = findDuplicates(samReaderResults);
        assertThat(duplicatesResults).isEqualTo(duplicatesExpected);
    }

    private Set<SAMRecord> findDuplicates(final SamReader samReaderResults) {
        Set<SAMRecord> duplicates = new HashSet<>();
        for (SAMRecord record : samReaderResults) {
            if (SAMFlag.getFlags(record.getFlags()).contains(SAMFlag.DUPLICATE_READ)) {
                duplicates.add(record);
            }
        }
        return duplicates;
    }
}
