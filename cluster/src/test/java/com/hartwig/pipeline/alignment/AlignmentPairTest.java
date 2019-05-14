package com.hartwig.pipeline.alignment;

import static com.hartwig.pipeline.testsupport.TestSamples.*;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.execution.JobStatus;

import org.junit.Test;

public class AlignmentPairTest {

    @Test
    public void findsTumorAndReferenceRegardlessOfOrderPassed() {
        AlignmentPair referenceThenTumor =
                AlignmentPair.of(output(simpleReferenceSample()), output(simpleTumorSample()));
        assertThat(referenceThenTumor.reference().sample()).isEqualTo(simpleReferenceSample());
        assertThat(referenceThenTumor.tumor().sample()).isEqualTo(simpleTumorSample());
        AlignmentPair tumorThenReference =
                AlignmentPair.of(output(simpleTumorSample()), output(simpleReferenceSample()));
        assertThat(tumorThenReference.reference().sample()).isEqualTo(simpleReferenceSample());
        assertThat(tumorThenReference.tumor().sample()).isEqualTo(simpleTumorSample());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgumentOnTwoReference() {
        AlignmentPair.of(output(simpleReferenceSample()), output(simpleReferenceSample()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgumentOnTwoTumor() {
        AlignmentPair.of(output(simpleTumorSample()), output(simpleTumorSample()));
    }

    public AlignmentOutput output(final Sample sample) {
        return AlignmentOutput.builder().status(JobStatus.SUCCESS).sample(sample).build();
    }

}