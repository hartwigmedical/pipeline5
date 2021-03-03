package com.hartwig.pipeline.transfer;

import static java.lang.String.format;

import java.io.File;
import java.util.Set;
import java.util.function.Consumer;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.MD5s;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.sbpapi.AddFileApiResponse;
import com.hartwig.pipeline.sbpapi.SbpFileMetadata;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpRun;
import com.hartwig.pipeline.transfer.sbp.ContentTypeCorrection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SbpFileApiUpdate implements Consumer<Blob> {

    private final static Logger LOGGER = LoggerFactory.getLogger(SbpFileApiUpdate.class);
    private final ContentTypeCorrection contentTypeCorrection;
    private final SbpRun sbpRun;
    private final Bucket sourceBucket;
    private final SbpRestApi sbpApi;
    private final Set<AddDatatype> addDatatypes;

    public SbpFileApiUpdate(final ContentTypeCorrection contentTypeCorrection, final SbpRun sbpRun, final Bucket sourceBucket,
            final SbpRestApi sbpApi, final Set<AddDatatype> addDatatypes) {
        this.contentTypeCorrection = contentTypeCorrection;
        this.sbpRun = sbpRun;
        this.sourceBucket = sourceBucket;
        this.sbpApi = sbpApi;
        this.addDatatypes = addDatatypes;
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
                        .hash(MD5s.asHex(blob.getMd5()))
                        .build();
                AddFileApiResponse fileResponse = sbpApi.postFile(metaData);

                for (AddDatatype addDatatype : addDatatypes) {
                    if (addDatatype.path().equals(blob.getName().substring(blob.getName().indexOf("/") + 1))) {
                        LOGGER.info("Applying [{}] for [{}]", addDatatype, blob.getName());
                        addDatatype.apply(sbpApi, fileResponse);
                    }
                }
            }
        }
    }

    private String extractDirectoryNameForSbp(String fullDestFilePath) {
        String parent = new File(fullDestFilePath.substring(fullDestFilePath.indexOf("/") + 1, fullDestFilePath.length() - 1)).getParent();
        return parent != null ? parent : "";
    }
}
