package com.hartwig.batch;

import static java.lang.String.format;

import static org.apache.commons.io.FileUtils.readLines;

import java.io.File;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.CommandLineOptions;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.metadata.RunMetadata;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.storage.StorageProvider;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchDispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchDispatcher.class);

    @Value.Immutable
    public interface StateTuple {
        String id();

        String url();

        Future<PipelineStatus> future();
    }

    public BatchDispatcher() {
    }

    public void runBatch(Arguments arguments) throws Exception {
        GoogleCredentials credentials = CredentialProvider.from(arguments).get();
        Storage storage = StorageProvider.from(arguments, credentials).get();

        // TODO promote to arguments
        String runPrefix = "ned-cram-conv";
        int maxConcurrency = 10;
        String urlList = "/home/ned/source/bam-batch-urls.list";

        ExecutorService executorService = Executors.newFixedThreadPool(maxConcurrency);
        Set<StateTuple> state = new HashSet<>();
        Set<String> urls = readLines(new File(urlList), "UTF-8").stream().filter(s -> !s.trim().isEmpty()).collect(Collectors.toSet());
        LOGGER.info("Running {} distinct input files with {} threads", urls.size(), maxConcurrency);
        int i = 0;
        for (String url : urls) {
            // TODO calculate padding from number of urls
            final String label = format("%03d", i + 1);
            RunMetadata runMetadata = () -> runPrefix + "-" + label;
            RuntimeBucket bucket = RuntimeBucket.from(storage, "batch", runMetadata, arguments);
            ComputeEngine compute = ComputeEngine.from(arguments, credentials);
            Future<PipelineStatus> future = executorService.submit(() -> new CramConverter(bucket, compute).convert(url));
            state.add(ImmutableStateTuple.builder().id(label).url(url).future(future).build());
            i++;
        }
        spawnProgessLogger(state);
        for (StateTuple job : state) {
            job.future().get();
        }
        StringBuilder report = new StringBuilder("EXECUTION REPORT\n\n");
        AtomicBoolean jobsFailed = new AtomicBoolean(false);
        state.stream().sorted(Comparator.comparing(stateTuple -> Integer.valueOf(stateTuple.id()))).forEach(stateTuple -> {
            try {
                report.append(format("  %s %s %s\n", stateTuple.id(), stateTuple.future().get(), stateTuple.url()));
                if (stateTuple.future().get() != PipelineStatus.SUCCESS) {
                    jobsFailed.set(true);
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        LOGGER.info("Batch completed");
        LOGGER.info(report.toString());
        System.exit(!jobsFailed.get() ? 0 : 1);
    }

    private void spawnProgessLogger(Set<StateTuple> state) {
        Thread progressLogger = new Thread(() -> {
            while (true) {
                int done = 0;
                int cancelled = 0;
                for (StateTuple stateTuple : state) {
                    if (stateTuple.future().isCancelled()) {
                        cancelled++;
                    } else if (stateTuple.future().isDone()) {
                        done++;
                    }
                }
                LOGGER.info("Job stats: {} running, {} finished, {} cancelled", state.size() - done - cancelled, done, cancelled);
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(30));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        });
        progressLogger.setDaemon(true);
        progressLogger.start();
    }

    public static final void main(String[] args) throws Exception {
        new BatchDispatcher().runBatch(CommandLineOptions.from(args));
    }
}
