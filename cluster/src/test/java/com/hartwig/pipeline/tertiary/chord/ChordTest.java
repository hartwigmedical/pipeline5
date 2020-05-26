package com.hartwig.pipeline.tertiary.chord;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class ChordTest extends TertiaryStageTest<ChordOutput> {

    @Override
    protected Stage<ChordOutput, SomaticRunMetadata> createVictim() {
        return new Chord(TestInputs.purpleOutput());
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.sv.vcf.gz", "tumor.purple.sv.vcf.gz"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("$TOOLS_DIR/chord/60.02_1.03/extractSigPredictHRD.R $TOOLS_DIR/chord/60.02_1.03 /data/output tumor "
                + "/data/input/tumor.purple.somatic.vcf.gz /data/input/tumor.purple.sv.vcf.gz");
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validateOutput(final ChordOutput output) {
        // no additional validation
    }
}