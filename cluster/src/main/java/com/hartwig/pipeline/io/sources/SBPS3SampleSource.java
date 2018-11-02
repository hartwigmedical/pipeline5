package com.hartwig.pipeline.io.sources;

import java.io.File;
import java.util.stream.Stream;

import com.amazonaws.services.s3.AmazonS3;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.SBPRestApi;
import com.hartwig.pipeline.io.SBPSampleReader;

public class SBPS3SampleSource implements SampleSource {

    private final SBPRestApi restApi;
    private final AmazonS3 s3;

    public SBPS3SampleSource(final SBPRestApi restApi, final AmazonS3 s3) {
        this.restApi = restApi;
        this.s3 = s3;
    }

    @Override
    public SampleData sample(final Arguments arguments, final RuntimeBucket runtimeBucket) {
        SBPSampleReader sbpSampleReader = new SBPSampleReader(restApi);
        Sample sample = sbpSampleReader.read(arguments.sbpApiSampleId()
                .orElseThrow(() -> new IllegalArgumentException("Arguments must "
                        + "contain an SBP id to use the SBP sample source. This looks like a programmatic mis-wiring somewhere")));
        long size = sample.lanes().stream().flatMap(lane -> Stream.of(lane.readsPath(), lane.matesPath())).mapToLong(this::size).sum();
        return SampleData.of(sample, size);
    }

    private long size(String filename) {
        File file = new File(filename);
        String bucket = file.getParent().replaceAll("/", "");
        return s3.getObject(bucket, file.getName()).getObjectMetadata().getContentLength();
    }
}
