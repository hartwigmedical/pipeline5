package com.hartwig.pipeline.metadata;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.sbpapi.ObjectMappers;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpRun;
import com.hartwig.pipeline.sbpapi.SbpSample;
import com.hartwig.pipeline.sbpapi.SbpSet;
import com.hartwig.pipeline.transfer.SbpFileTransfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SbpSomaticMetadataApi implements SomaticMetadataApi {

    static final String SNP_CHECK = "SnpCheck";
    static final String FAILED = "Failed";
    private static final String UPLOADING = "Uploading";
    private static final String REF = "ref";
    private static final String TUMOR = "tumor";
    private final static Logger LOGGER = LoggerFactory.getLogger(SomaticMetadataApi.class);
    private final Arguments arguments;
    private final int sbpRunId;
    private final SbpRestApi sbpRestApi;
    private final SbpFileTransfer publisher;
    private final LocalDateTime now;

    SbpSomaticMetadataApi(final Arguments arguments, final int sbpRunId, final SbpRestApi sbpRestApi, final SbpFileTransfer publisher,
            final LocalDateTime now) {
        this.arguments = arguments;
        this.sbpRunId = sbpRunId;
        this.sbpRestApi = sbpRestApi;
        this.publisher = publisher;
        this.now = now;
    }

    @Override
    public SomaticRunMetadata get() {
        try {
            SbpRun sbpRun = getSbpRun();
            SbpSet sbpSet = sbpRun.set();
            List<SbpSample> samplesBySet =
                    ObjectMappers.get().readValue(sbpRestApi.getSample(sbpSet.id()), new TypeReference<List<SbpSample>>() {
                    });
            SbpSample reference = find(REF, sbpSet.id(), samplesBySet);
            SbpSample tumor = find(TUMOR, sbpSet.id(), samplesBySet);
            return SomaticRunMetadata.builder()
                    .runName(RunTag.apply(arguments, sbpSet.name()))
                    .tumor(SingleSampleRunMetadata.builder()
                            .sampleName(tumor.name())
                            .sampleId(tumor.barcode())
                            .type(SingleSampleRunMetadata.SampleType.TUMOR)
                            .build())
                    .reference(SingleSampleRunMetadata.builder()
                            .sampleName(reference.name())
                            .sampleId(reference.barcode())
                            .type(SingleSampleRunMetadata.SampleType.REFERENCE)
                            .build())
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SbpRun getSbpRun() {
        try {
            return ObjectMappers.get().readValue(sbpRestApi.getRun(sbpRunId), SbpRun.class);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private SbpSample find(final String type, final String setName, final List<SbpSample> samplesBySet) throws IOException {
        List<SbpSample> sampleByType = samplesBySet.stream().filter(sample -> sample.type().equals(type)).collect(Collectors.toList());
        if (sampleByType.size() != 1) {
            throw new IllegalStateException(String.format("Could not find a single [%s] sample for run id [%s] through set [%s]. Found [%s]",
                    type,
                    sbpRunId,
                    setName,
                    sampleByType.size()));
        }
        return sampleByType.get(0);
    }

    @Override
    public void complete(final PipelineStatus status, SomaticRunMetadata metadata) {
        String runIdAsString = String.valueOf(sbpRunId);
        SbpRun sbpRun = getSbpRun();
        String sbpBucket = sbpRun.bucket();
        if (sbpBucket != null) {
            LOGGER.info("Recording pipeline completion with status [{}]", status);
            try {
                sbpRestApi.updateRunStatus(runIdAsString, UPLOADING, sbpBucket);
                publisher.publish(metadata, sbpRun, sbpBucket);
                sbpRestApi.updateRunStatus(runIdAsString, status == PipelineStatus.SUCCESS ? SNP_CHECK : FAILED, sbpBucket);
            } catch (Exception e) {
                sbpRestApi.updateRunStatus(runIdAsString, FAILED, sbpBucket);
                throw e;
            }
        } else {
            throw new IllegalStateException(String.format(
                    "Run [%s] has no predefined bucket for output. The bucket should be populated externally to P5, check configuration in "
                            + "SBP API.",
                    sbpRunId));
        }
    }
}
