package com.hartwig.pipeline.transfer.google;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.storagetransfer.v1.Storagetransfer;
import com.google.api.services.storagetransfer.v1.StoragetransferScopes;
import com.google.api.services.storagetransfer.v1.model.Date;
import com.google.api.services.storagetransfer.v1.model.GcsData;
import com.google.api.services.storagetransfer.v1.model.ListOperationsResponse;
import com.google.api.services.storagetransfer.v1.model.ObjectConditions;
import com.google.api.services.storagetransfer.v1.model.Schedule;
import com.google.api.services.storagetransfer.v1.model.TransferJob;
import com.google.api.services.storagetransfer.v1.model.TransferOptions;
import com.google.api.services.storagetransfer.v1.model.TransferSpec;
import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class GoogleArchiver {
    private static final String APPLICATION_NAME = "google-archive-transfer";
    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleArchiver.class);
    private final String sourceBucket;
    private final String destinationBucket;
    private final Arguments arguments;

    public GoogleArchiver(Arguments arguments) {
        this.sourceBucket = arguments.patientReportBucket();
        this.destinationBucket = arguments.archiveBucket();
        this.arguments = arguments;
    }

    public void transfer(SomaticRunMetadata metadata) {
        HttpTransport http = Utils.getDefaultTransport();
        JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
        try {
            GoogleCredential credential = credentialsFromFile(arguments);
            HttpRequestInitializer initialiser = new RetryHttpInitializerWrapper(credential);
            Storagetransfer transfer =
                    new Storagetransfer.Builder(http, jsonFactory, initialiser).setApplicationName(APPLICATION_NAME).build();
            runTransfer(transfer, metadata.runName());
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to complete transfer to archive folder", ioe);
        }
    }

    private void runTransfer(Storagetransfer transfer, String folder) throws IOException {
        Schedule schedule = new Schedule();
        Date startDate = new Date();
        ZonedDateTime utc = ZonedDateTime.now(ZoneId.of("UTC"));
        startDate.setDay(utc.getDayOfMonth());
        startDate.setMonth(utc.getMonthValue());
        startDate.setYear(utc.getYear());

        schedule.setScheduleStartDate(startDate);
        schedule.setScheduleEndDate(startDate);

        String description = jobDescription(folder, utc);
        TransferJob job = new TransferJob().setTransferSpec(new TransferSpec().setGcsDataSource(new GcsData().setBucketName(sourceBucket))
                .setGcsDataSink(new GcsData().setBucketName(destinationBucket))
                .setObjectConditions(new ObjectConditions().setIncludePrefixes(ImmutableList.of(folder)))
                .setTransferOptions(new TransferOptions().setDeleteObjectsFromSourceAfterTransfer(false)))
                .setProjectId(arguments.project())
                .setSchedule(schedule).setDescription(description)
                .setStatus("ENABLED");
        LOGGER.info("Creating transfer job [{}]", description);
        TransferJob createdJob = transfer.transferJobs().create(job).execute();

        Failsafe.with(new RetryPolicy<>().withMaxRetries(-1).withDelay(Duration.ofSeconds(5)).handleResult(Boolean.FALSE)).get(() -> {
            ListOperationsResponse transferOperations = transfer.transferOperations()
                    .list("transferOperations")
                    .setFilter(format("{\"project_id\": \"%s\", \"job_names\": [\"%s\"]}", arguments.project(), createdJob.getName()))
                    .execute();
            if (transferOperations.getOperations().size() > 0) {
                LOGGER.info("Transfer job [{}] completed", description);
                return transferOperations.getOperations().get(0).getMetadata().get("status").equals("SUCCESS");
            }
            LOGGER.info("Continuing to wait for completion of transfer operation");
            return Boolean.FALSE;
        });
    }

    private String jobDescription(String folder, ZonedDateTime zonedDateTime) {
        String formattedTime = zonedDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd-kkmm"));
        return format("%s-%s---%s_%s", sourceBucket, folder, destinationBucket, formattedTime);
    }

    private GoogleCredential credentialsFromFile(Arguments arguments) throws IOException {
        GoogleCredential credential = GoogleCredential.fromStream(FileUtils.openInputStream(new File(arguments.archivePrivateKeyPath())));
        return credential.createScoped(StoragetransferScopes.all());
    }
}
