package com.hartwig.bcl2fastq.metadata;

import static java.lang.String.format;

import java.util.Base64;
import java.util.function.Consumer;

import com.hartwig.bcl2fastq.results.Conversion;
import com.hartwig.bcl2fastq.results.ConvertedFastq;
import com.hartwig.bcl2fastq.results.ConvertedSample;

import org.apache.commons.codec.binary.Hex;

public class FastqMetadataRegistration implements Consumer<Conversion> {

    private final SbpFastqMetadataApi sbpApi;
    private final String outputBucket;

    public FastqMetadataRegistration(final SbpFastqMetadataApi sbpApi, final String outputBucket) {
        this.sbpApi = sbpApi;
        this.outputBucket = outputBucket;
    }

    @Override
    public void accept(final Conversion conversion) {
        SbpFlowcell sbpFlowcell = sbpApi.getFlowcell(conversion.flowcell());
        if (sbpFlowcell != null) {
            boolean flowcellQCPass =
                    QualityControl.errorsInLogs() && QualityControl.undeterminedReadPercentage() && QualityControl.minimumYield();
            if (flowcellQCPass) {
                for (ConvertedSample sample : conversion.samples()) {
                    SbpSample sbpSample = sbpApi.findOrCreate(sample.barcode(), sample.project());
                    updateSampleYieldAndStatus(sample, sbpSample);
                    for (ConvertedFastq convertedFastq : sample.fastq()) {
                        SbpLane sbpLane = sbpApi.findOrCreate(SbpLane.builder()
                                .flowcell_id(sbpFlowcell.id())
                                .name(lane(convertedFastq.id().lane()))
                                .build());

                        sbpApi.findOrCreate(SbpFastq.builder()
                                .sample_id(sbpSample.id().orElseThrow())
                                .lane_id(sbpLane.id().orElseThrow())
                                .bucket(outputBucket)
                                .name_r1(convertedFastq.outputPathR1())
                                .size_r1(convertedFastq.sizeR1())
                                .hash_r1(convertMd5ToSbpFormat(convertedFastq.md5R1()))
                                .name_r1(convertedFastq.outputPathR1())
                                .size_r1(convertedFastq.sizeR1())
                                .hash_r1(convertMd5ToSbpFormat(convertedFastq.md5R1()))
                                .qc_pass(QualityControl.minimumQ30())
                                .build());
                    }
                }
            }
        }
    }

    private String convertMd5ToSbpFormat(String originalMd5) {
        return new String(Hex.encodeHex(Base64.getDecoder().decode(originalMd5)));
    }

    private String lane(final int laneNumber) {
        return format("L00%s", laneNumber);
    }

    public void updateSampleYieldAndStatus(final ConvertedSample sample, final SbpSample sbpSample) {
        final double sampleQ30 = sample.yieldQ30() / (double) sample.yield() * 100;
        ImmutableSbpSample.Builder sampleUpdate = SbpSample.builder().from(sbpSample).yld(sample.yield()).q30(sampleQ30);
        if (sbpSample.yld_req().map(yr -> yr < sample.yield()).orElse(true) && sbpSample.q30_req().map(qr -> qr < sampleQ30).orElse(true)) {
            sampleUpdate.status("Ready");
        } else {
            sampleUpdate.status("Insufficient Quality");
        }
        sbpApi.updateSample(sampleUpdate.build());
    }
}
