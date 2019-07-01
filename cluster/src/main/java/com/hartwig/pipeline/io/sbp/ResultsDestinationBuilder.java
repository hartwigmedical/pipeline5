package com.hartwig.pipeline.io.sbp;

import static java.lang.String.format;

import java.util.Calendar;
import java.util.TimeZone;

public class ResultsDestinationBuilder {
    private final String bucketPrefix;
    private final String providerDescriptor;

    public ResultsDestinationBuilder(final String providerDescriptor, final String bucketPrefix) {
        this.providerDescriptor = providerDescriptor;
        this.bucketPrefix = bucketPrefix;
    }

    public CloudFile buildFor(String sbpSetName) {
        Calendar date = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        String bucketName = format("%s-%d-%d", bucketPrefix, date.get(Calendar.YEAR), date.get(Calendar.WEEK_OF_YEAR));
        return CloudFile.builder().provider(providerDescriptor).bucket(bucketName).path(sbpSetName).build();
    }
}
