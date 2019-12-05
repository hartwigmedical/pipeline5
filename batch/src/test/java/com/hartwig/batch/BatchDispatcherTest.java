package com.hartwig.batch;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.batch.input.InputParser;
import com.hartwig.batch.operations.CommandDescriptor;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.testsupport.Resources;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BatchDispatcherTest {
    @Test
    @SuppressWarnings("unchecked")
    public void shouldCallOperationOnceForEachObjectInListOfInputs() throws Exception {
        String opName = "operationName";
        BatchOperation mockOp = mock(BatchOperation.class);
        when(mockOp.descriptor()).thenReturn(CommandDescriptor.of(opName, "description"));
        InstanceFactory instanceFactory = mock(InstanceFactory.class);
        InputParser inputParser = mock(InputParser.class);
        GoogleCredentials credentials = mock(GoogleCredentials.class);
        ComputeEngine computeEngine = mock(ComputeEngine.class);
        Storage storage = mock(Storage.class);
        Bucket bucket = mock(Bucket.class);
        ExecutorService executorService = mock(ExecutorService.class);

        BatchArguments arguments = defaultArgs(opName);
        when(storage.get(arguments.outputBucket())).thenReturn(bucket);
        when(inputParser.parse()).thenReturn(asList(singletonList(mock(InputFileDescriptor.class)), singletonList(mock(InputFileDescriptor.class))));
        when(executorService.submit((Callable) any())).thenReturn(mock(Future.class)).thenReturn(mock(Future.class));

        BatchDispatcher dispatcher = new BatchDispatcher(arguments, instanceFactory, inputParser, credentials,
                computeEngine, storage, executorService);

        dispatcher.runBatch();

        verify(executorService, times(2)).submit((Callable) any());

    }

    private BatchArguments defaultArgs(String operation) {
        BatchArguments baseWithDefaults = BatchArguments.from(new String[]{operation,
                "-" + BatchArguments.PRIVATE_KEY_PATH, "irrelevant",
                "-" + BatchArguments.SERVICE_ACCOUNT_EMAIL, "irrelevant",
                "-" + BatchArguments.INPUT_FILE, Resources.testResource("batch-dispatcher/batch_descriptor.json"),
                "-" + BatchArguments.OUTPUT_BUCKET, "irrelevant"});
        return BatchArguments.builder().from(baseWithDefaults)
                .inputFormat("json")
                .build();
    }
}