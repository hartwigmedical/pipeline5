package com.hartwig.pipeline.upload;

import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SBPS3BamSink implements BamSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(SBPS3BamSink.class);
    private static final String ROOT_BUCKET = "hmf-bam-storage";
    private final AmazonS3 s3Client;
    private final SBPRestApi sbpRestApi;
    private final int sbpSampleId;

    private SBPS3BamSink(final AmazonS3 s3Client, final SBPRestApi sbpRestApi, final int sbpSampleId) {
        this.s3Client = s3Client;
        this.sbpRestApi = sbpRestApi;
        this.sbpSampleId = sbpSampleId;
    }

    public static SBPS3BamSink newInstance(final AmazonS3 s3Client, final SBPRestApi sbpRestApi, final int sbpSampleId) {
        return new SBPS3BamSink(s3Client, sbpRestApi, sbpSampleId);
    }

    @Override
    public void save(final Sample sample, RuntimeBucket runtimeBucket, final InputStream bamStream) {
        String directory = ROOT_BUCKET + "/" + sample.barcode();
        String bamFile = sample.name() + ".bam";
        DigestInputStream md5InputStream = md5Stream(bamStream);
        s3Client.putObject(directory, bamFile, md5InputStream, new ObjectMetadata());
        String hash = new String(md5InputStream.getMessageDigest().digest());
        ObjectMetadata existing = s3Client.getObject(directory, bamFile).getObjectMetadata();
        sbpRestApi.patchBam(sbpSampleId,
                BamMetadata.builder()
                        .bucket(ROOT_BUCKET)
                        .directory(sample.barcode())
                        .filename(bamFile)
                        .filesize(existing.getContentLength())
                        .hash(hash)
                        .build());
        LOGGER.info("Downloaded BAM file to [s3://{}/{}]", directory, bamFile);
    }

    @NotNull
    private static DigestInputStream md5Stream(final InputStream bamStream) {
        try {
            return new DigestInputStream(bamStream, MessageDigest.getInstance("MD5"));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}