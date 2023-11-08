package com.hartwig.pipeline.input;

import java.util.Optional;

import com.hartwig.pdl.PipelineInput;
import com.hartwig.pdl.SampleInput;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.input.SingleSampleRunMetadata.SampleType;

public class MetadataProvider {

    private final Arguments arguments;
    private final PipelineInput pipelineInput;

    public MetadataProvider(final Arguments arguments, final PipelineInput pipelineInput) {
        this.arguments = arguments;
        this.pipelineInput = pipelineInput;
    }

    public SomaticRunMetadata get() {
        String setName = RunTag.apply(arguments, pipelineInput.setName());
        Optional<SampleInput> reference = Inputs.sample(pipelineInput, SampleType.REFERENCE);
        Optional<SampleInput> tumor = Inputs.sample(pipelineInput, SampleType.TUMOR);

        return SomaticRunMetadata.builder()
                .set(setName)
                .bucket(arguments.outputBucket())
                .maybeTumor(tumor.map(t -> SingleSampleRunMetadata.builder()
                        .bucket(arguments.outputBucket())
                        .set(setName)
                        .type(SingleSampleRunMetadata.SampleType.TUMOR)
                        .barcode(barcodeOrSampleName(t))
                        .sampleName(t.name())
                        .turquoiseSubject(t.turquoiseSubject())
                        .primaryTumorDoids(t.primaryTumorDoids())
                        .samplingDate(t.samplingDate())
                        .build()))
                .maybeReference(reference.map(r -> SingleSampleRunMetadata.builder()
                        .bucket(arguments.outputBucket())
                        .set(setName)
                        .type(SingleSampleRunMetadata.SampleType.REFERENCE)
                        .barcode(barcodeOrSampleName(r))
                        .sampleName(r.name())
                        .turquoiseSubject(r.turquoiseSubject())
                        .build()))
                .maybeExternalIds(pipelineInput.operationalReferences())
                .build();
    }

    private static String barcodeOrSampleName(SampleInput sampleInput) {
        return sampleInput.barcode().equals(SampleInput.NOT_APPLICABLE) ? sampleInput.name() : sampleInput.barcode();
    }
}