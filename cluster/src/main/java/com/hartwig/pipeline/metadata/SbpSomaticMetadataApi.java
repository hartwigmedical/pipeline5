package com.hartwig.pipeline.metadata;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.sbpapi.ObjectMappers;
import com.hartwig.pipeline.sbpapi.SbpIni;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpRun;
import com.hartwig.pipeline.sbpapi.SbpSample;
import com.hartwig.pipeline.sbpapi.SbpSet;
import com.hartwig.pipeline.transfer.SbpFileTransfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SbpSomaticMetadataApi implements SomaticMetadataApi {

    static final String SUCCESS = "Success";
    static final String SNP_CHECK = "SnpCheck";
    static final String FAILED = "Failed";
    private static final String UPLOADING = "Uploading";
    private static final String REF = "ref";
    private static final String TUMOR = "tumor";
    private final static Logger LOGGER = LoggerFactory.getLogger(SomaticMetadataApi.class);
    private static final String SINGLE_SAMPLE_INI = "SingleSample";
    private final Arguments arguments;
    private final int sbpRunId;
    private final SbpRestApi sbpRestApi;
    private final SbpFileTransfer publisher;

    SbpSomaticMetadataApi(final Arguments arguments, final int sbpRunId, final SbpRestApi sbpRestApi, final SbpFileTransfer publisher) {
        this.arguments = arguments;
        this.sbpRunId = sbpRunId;
        this.sbpRestApi = sbpRestApi;
        this.publisher = publisher;
    }

    @Override
    public SomaticRunMetadata get() {
        try {
            SbpRun sbpRun = getSbpRun();
            SbpSet sbpSet = sbpRun.set();
            List<SbpSample> samplesBySet =
                    ObjectMappers.get().readValue(sbpRestApi.getSample(sbpSet.id()), new TypeReference<List<SbpSample>>() {
                    });

            SingleSampleRunMetadata reference = find(REF, samplesBySet).map(referenceSample -> toMetadata(referenceSample,
                    SingleSampleRunMetadata.SampleType.REFERENCE))
                    .orElseThrow(() -> new IllegalStateException(String.format("No reference sample found in SBP for set [%s]",
                            sbpSet.name())));
            SbpIni ini = findIni(sbpRun, getInis());
            if (ini.name().startsWith(SINGLE_SAMPLE_INI)) {
                LOGGER.info("Somatic run is using single sample configuration. No algorithms will be run, just transfer and cleanup");
                return SomaticRunMetadata.builder().runName(sbpSet.name()).reference(reference).build();
            } else {
                SingleSampleRunMetadata tumor = find(TUMOR, samplesBySet).map(referenceSample -> toMetadata(referenceSample,
                        SingleSampleRunMetadata.SampleType.TUMOR))
                        .orElseThrow((() -> new IllegalStateException(String.format(
                                "No tumor sample found in SBP for set [%s] and this run " + "was not marked as single sample",
                                sbpSet.name()))));
                return SomaticRunMetadata.builder().runName(sbpSet.name()).reference(reference).maybeTumor(tumor).build();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<SbpIni> getInis() throws IOException {
        return ObjectMappers.get().readValue(sbpRestApi.getInis(), new TypeReference<List<SbpIni>>() {
        });
    }

    private SbpIni findIni(final SbpRun sbpRun, final List<SbpIni> inis) {
        return inis.stream()
                .filter(i -> i.id() == sbpRun.ini_id())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(String.format(
                        "Unable to find ini [%s] referenced in run [%s]. " + "Check the configuration in the SBP API",
                        sbpRun.ini_id(),
                        sbpRun.id())));
    }

    private static ImmutableSingleSampleRunMetadata toMetadata(final SbpSample sample, final SingleSampleRunMetadata.SampleType type) {
        return SingleSampleRunMetadata.builder()
                .sampleName(sample.name())
                .sampleId(sample.barcode())
                .entityId(sample.id())
                .type(type)
                .build();
    }

    private SbpRun getSbpRun() {
        try {
            return ObjectMappers.get().readValue(sbpRestApi.getRun(sbpRunId), SbpRun.class);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private Optional<SbpSample> find(final String type, final List<SbpSample> samplesBySet) throws IOException {
        return samplesBySet.stream().filter(sample -> sample.type().equals(type)).findFirst();
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
                sbpRestApi.updateRunStatus(runIdAsString, status == PipelineStatus.SUCCESS ? successStatus() : FAILED, sbpBucket);
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

    private String successStatus() {
        return arguments.shallow() ? SUCCESS : SNP_CHECK;
    }
}
