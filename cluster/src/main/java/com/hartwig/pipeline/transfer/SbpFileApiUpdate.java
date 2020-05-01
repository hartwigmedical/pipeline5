package com.hartwig.pipeline.transfer;

import static java.lang.String.format;

import java.io.File;
import java.util.Base64;
import java.util.function.Consumer;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.metadata.AdditionalApiCalls;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.sbpapi.FileResponse;
import com.hartwig.pipeline.sbpapi.SbpFileMetadata;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpRun;
import com.hartwig.pipeline.transfer.sbp.ContentTypeCorrection;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SbpFileApiUpdate implements Consumer<Blob> {

    private final static Logger LOGGER = LoggerFactory.getLogger(SbpFileApiUpdate.class);
    private final ContentTypeCorrection contentTypeCorrection;
    private final AdditionalApiCalls additionalApiCalls;
    private final SbpRun sbpRun;
    private final Bucket sourceBucket;
    private final SbpRestApi sbpApi;

    public SbpFileApiUpdate(final ContentTypeCorrection contentTypeCorrection, final AdditionalApiCalls additionalApiCalls,
            final SbpRun sbpRun, final Bucket sourceBucket, final SbpRestApi sbpApi) {
        this.contentTypeCorrection = contentTypeCorrection;
        this.additionalApiCalls = additionalApiCalls;
        this.sbpRun = sbpRun;
        this.sourceBucket = sourceBucket;
        this.sbpApi = sbpApi;
    }

    @Override
    public void accept(final Blob blob) {
        if (!blob.getName().endsWith(PipelineResults.STAGING_COMPLETE)) {
            if (blob.getMd5() == null) {
                throw new IllegalStateException(format("Object gs://%s/%s has a null MD5", sourceBucket.getName(), blob.getName()));
            } else {
                contentTypeCorrection.apply(blob);
                SbpFileMetadata metaData = SbpFileMetadata.builder()
                        .directory(extractDirectoryNameForSbp(blob.getName()))
                        .run_id(Integer.parseInt(sbpRun.id()))
                        .filename(new File(blob.getName()).getName())
                        .filesize(blob.getSize())
                        .hash(convertMd5ToSbpFormat(blob.getMd5()))
                        .build();
                FileResponse response = sbpApi.postFile(metaData);
                additionalApiCalls.apply(sbpApi, blob.getName(), response);
            }
        }
    }

    private String convertMd5ToSbpFormat(String originalMd5) {
        return new String(Hex.encodeHex(Base64.getDecoder().decode(originalMd5)));
    }

    private String extractDirectoryNameForSbp(String fullDestFilePath) {
        String parent = new File(fullDestFilePath.substring(fullDestFilePath.indexOf("/") + 1, fullDestFilePath.length() - 1)).getParent();
        return parent != null ? parent : "";
    }
}
