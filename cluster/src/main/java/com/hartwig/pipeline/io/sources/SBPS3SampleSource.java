package com.hartwig.pipeline.io.sources;

import java.util.stream.Stream;

import com.amazonaws.services.s3.AmazonS3;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.io.sbp.SBPSampleReader;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SBPS3SampleSource implements SampleSource {

    private final Logger LOGGER = LoggerFactory.getLogger(SampleSource.class);

    private final AmazonS3 s3;
    private final SBPSampleReader sbpSampleReader;

    public SBPS3SampleSource(final AmazonS3 s3, final SBPSampleReader sbpSampleReader) {
        this.s3 = s3;
        this.sbpSampleReader = sbpSampleReader;
    }

    @Override
    public SampleData sample(final SingleSampleRunMetadata metadata, final Arguments arguments) {
          Sample sample = sbpSampleReader.read(arguments.sbpApiSampleId()
                .orElseThrow(() -> new IllegalArgumentException("Arguments must "
                        + "contain an SBP id to use the SBP sample source. This looks like a programmatic mis-wiring somewhere")));
        long size = sample.lanes()
                .stream()
                .flatMap(lane -> Stream.of(lane.firstOfPairPath(), lane.secondOfPairPath()))
                .mapToLong(this::size)
                .sum();
        return SampleData.of(sample, size);
    }

    private long size(String filename) {
        String[] split = filename.split("/", 2);
        String bucket = split[0];
        String object = split[1];
        LOGGER.info("Requesting content length of S3 for object [{}] in bucket [{}]", object, bucket);
        return s3.getObject(bucket, object).getObjectMetadata().getContentLength();
    }
}
