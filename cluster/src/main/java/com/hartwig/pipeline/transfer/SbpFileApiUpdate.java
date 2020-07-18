package com.hartwig.pipeline.transfer;

import static java.lang.String.format;

import java.io.File;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.metadata.ApiFileOperation;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.sbpapi.AddFileApiResponse;
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
    private final SbpRun sbpRun;
    private final Bucket sourceBucket;
    private final SbpRestApi sbpApi;
    private final Set<ApiFileOperation> fileOperations;

    public SbpFileApiUpdate(final ContentTypeCorrection contentTypeCorrection, final SbpRun sbpRun, final Bucket sourceBucket,
            final SbpRestApi sbpApi, final PipelineState pipelineState) {
        this.contentTypeCorrection = contentTypeCorrection;
        this.sbpRun = sbpRun;
        this.sourceBucket = sourceBucket;
        this.sbpApi = sbpApi;
        fileOperations = new HashSet<>();
        pipelineState.stageOutputs().forEach(stageOutput -> fileOperations.addAll(stageOutput.furtherOperations()));
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
                AddFileApiResponse fileResponse = sbpApi.postFile(metaData);

                for (ApiFileOperation fileOperation : fileOperations) {
                    if (fileOperation.path().equals(blob.getName().substring(blob.getName().indexOf("/") + 1))) {
                        LOGGER.info("Applying [{}] for [{}]", fileOperation, blob.getName());
                        fileOperation.apply(sbpApi, fileResponse);
                    }
                }
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
