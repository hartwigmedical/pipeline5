package com.hartwig.pipeline.report;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.StageOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatientReport {

    private static final Logger LOGGER = LoggerFactory.getLogger(PatientReport.class);

    private final Storage storage;
    private final Bucket reportBucket;
    private final List<ReportComponent> components = new ArrayList<>();

    PatientReport(final Storage storage, final Bucket reportBucket) {
        this.storage = storage;
        this.reportBucket = reportBucket;
    }

    public <T extends StageOutput> T add(T stageOutput) {
        components.addAll(stageOutput.reportComponents());
        return stageOutput;
    }

    public void compose(String setName) {
        components.forEach(component -> {
            try {
                component.addToReport(storage, reportBucket, setName);
            } catch (Exception e) {
                LOGGER.error(format("Unable add component [%s] to the final patient report.", component.getClass().getSimpleName()), e);
            }
        });
    }
}
