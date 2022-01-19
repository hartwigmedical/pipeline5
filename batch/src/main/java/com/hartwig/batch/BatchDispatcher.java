package com.hartwig.batch;

import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Comparator.nullsLast;
import static java.util.stream.Collectors.toList;

import java.io.FileInputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.ApiException;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputParser;
import com.hartwig.batch.input.InputParserProvider;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.GoogleComputeEngine;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.storage.StorageProvider;
import com.hartwig.pipeline.tools.Versions;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class BatchDispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchDispatcher.class);
    private static final String LOG_NAME = "run.log";
    private final BatchArguments arguments;
    private final InstanceFactory instanceFactory;
    private final InputParserProvider parserProvider;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final ExecutorService executorService;
    private final Labels labels;

    @Value.Immutable
    interface StateTuple {
        String id();

        InputBundle inputs();

        Future<PipelineStatus> future();

        private static ImmutableStateTuple.Builder builder() {
            return ImmutableStateTuple.builder();
        }
    }

    BatchDispatcher(BatchArguments arguments, InstanceFactory instanceFactory, InputParserProvider parserProvider,
            ComputeEngine computeEngine, Storage storage, ExecutorService executorService, Labels labels) {
        this.arguments = arguments;
        this.instanceFactory = instanceFactory;
        this.parserProvider = parserProvider;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.executorService = executorService;
        this.labels = labels;
    }

    boolean runBatch() throws Exception {
        LOGGER.info("Invoked with arguments: [{}]", arguments);
        Versions.printAll();
        Set<StateTuple> state = new HashSet<>();
        InputParser inputParser = parserProvider.from(instanceFactory.get());
        List<InputBundle> inputs = inputParser.parse(arguments.inputFile(), arguments.project());

        LOGGER.info("Running {} jobs with up to {} concurrent VMs", inputs.size(), arguments.concurrency());
        confirmOutputBucketExists(storage);
        int i = 0;
        String paddingFormat = format("%%0%dd", String.valueOf(inputs.size()).length());
        LOGGER.info("Writing output to bucket [{}]", arguments.outputBucket());
        Map<String, InputBundle> jobAsString = new HashMap<>();
        for (InputBundle operationInputs : inputs) {
            final String label = format(paddingFormat, i + 1);
            RuntimeFiles executionFlags = RuntimeFiles.of(label);
            RuntimeBucket outputBucket = RuntimeBucket.from(storage, arguments.outputBucket(), label, arguments, labels);
            BashStartupScript startupScript = BashStartupScript.of(outputBucket.name(), executionFlags);
            Future<PipelineStatus> future = executorService.submit(() -> {
                try {
                    VirtualMachineJobDefinition jobDefinition = Failsafe.with(new RetryPolicy<>().handle(ApiException.class)
                            .withDelay(Duration.ofSeconds(10))
                            .withJitter(0.5)
                            .withMaxAttempts(250)).get(() -> {
                        try {
                            return instanceFactory.get().execute(operationInputs, outputBucket, startupScript, executionFlags);
                        } catch (ApiException e) {
                            LOGGER.warn("Encountered API exception when building job {}", label);
                            throw e;
                        }
                    });
                    return computeEngine.submit(outputBucket, jobDefinition, label);
                } catch (Exception e) {
                    LOGGER.warn("Unexpected exception running operation [{}]", label, e);
                    return PipelineStatus.FAILED;
                }
            });
            state.add(StateTuple.builder().id(label).inputs(operationInputs).future(future).build());
            jobAsString.put(label, operationInputs);
            i++;
        }
        spawnProgressLogger(state);
        RuntimeBucket outputBucket = RuntimeBucket.from(storage, arguments.outputBucket(), "", arguments, labels);
        Bucket bucketRoot = outputBucket.getUnderlyingBucket();
        bucketRoot.create("job.json", new ObjectMapper().writeValueAsBytes(jobAsString));
        for (StateTuple job : state) {

            job.future().get();
        }
        StringBuilder report = new StringBuilder("EXECUTION REPORT\n\n");

        boolean jobsFailed = false;
        List<StateTuple> tuples = state.stream().sorted(comparing(stateTuple -> Integer.valueOf(stateTuple.id()))).collect(toList());
        for (StateTuple stateTuple : tuples) {
            report.append(String.format("  %s %s %s\n",
                    stateTuple.id(),
                    stateTuple.future().get(),
                    stateTuple.inputs().get().inputValue()));
            if (stateTuple.future().get() != PipelineStatus.SUCCESS) {
                jobsFailed = true;
            }
        }
        LOGGER.info("Batch completed");
        LOGGER.info(report.toString());

        bucketRoot.create(LOG_NAME, new FileInputStream(LOG_NAME));
        return !jobsFailed;
    }

    private void confirmOutputBucketExists(Storage storage) {
        if (storage.get(arguments.outputBucket()) == null) {
            throw new IllegalStateException(format("Output bucket [{%s}] does not exist", arguments.outputBucket()));
        }
    }

    private void spawnProgressLogger(Set<StateTuple> state) {
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
                LOGGER.info("Job stats: {} pending, {} finished, {} cancelled", state.size() - done - cancelled, done, cancelled);
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(30));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        progressLogger.setDaemon(true);
        progressLogger.start();
    }

    public static void main(String[] args) throws Exception {
        BatchArguments arguments = BatchArguments.from(args);
        GoogleCredentials credentials = arguments.privateKeyPath().isPresent()
                ? CredentialProvider.from(arguments).get()
                : GoogleCredentials.getApplicationDefault();
        Labels labels = Labels.of(arguments);
        ComputeEngine compute = GoogleComputeEngine.from(arguments, credentials, true, labels);
        Storage storage = StorageProvider.from(arguments, credentials).get();
        boolean success = new BatchDispatcher(arguments,
                InstanceFactory.from(arguments),
                new InputParserProvider(),
                compute,
                storage,
                Executors.newFixedThreadPool(arguments.concurrency()),
                labels).runBatch();
        System.exit(success ? 0 : 1);
    }
}
