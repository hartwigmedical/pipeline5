package com.hartwig.batch;

import static java.util.Arrays.asList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.batch.input.InputParser;
import com.hartwig.batch.input.InputParserProvider;
import com.hartwig.batch.operations.OperationDescriptor;
import com.hartwig.batch.testsupport.TestingArguments;
import com.hartwig.pipeline.execution.vm.GoogleComputeEngine;

import org.junit.Test;
import org.mockito.Mockito;

public class BatchDispatcherTest {
    @Test
    @SuppressWarnings("unchecked")
    public void shouldCallOperationOnceForEachObjectInListOfInputs() throws Exception {
        String opName = "operationName";
        BatchOperation mockOp = mock(BatchOperation.class);
        when(mockOp.descriptor()).thenReturn(OperationDescriptor.of(opName, "description",
                OperationDescriptor.InputType.FLAT));
        InstanceFactory instanceFactory = mock(InstanceFactory.class);
        InputParserProvider inputParserProvider = mock(InputParserProvider.class);
        InputParser inputParser = mock(InputParser.class);
        BatchOperation operation = mock(BatchOperation.class);
        GoogleComputeEngine computeEngine = mock(GoogleComputeEngine.class);
        Storage storage = mock(Storage.class);
        Bucket bucket = mock(Bucket.class);
        ExecutorService executorService = mock(ExecutorService.class);
        InputBundle bundle1 = mock(InputBundle.class);
        InputBundle bundle2 = mock(InputBundle.class);

        BatchArguments arguments = TestingArguments.defaultArgs(opName);
        when(inputParserProvider.from(any())).thenReturn(inputParser);
        when(inputParser.parse(any(), any())).thenReturn(asList(bundle1, bundle2));
        when(bundle1.get()).thenReturn(mock(InputFileDescriptor.class));
        when(bundle2.get()).thenReturn(mock(InputFileDescriptor.class));
        when(executorService.submit(Mockito.<Callable<?>>any())).thenReturn(mock(Future.class)).thenReturn(mock(Future.class));

        when(storage.get(arguments.outputBucket())).thenReturn(bucket);

        BatchDispatcher dispatcher = new BatchDispatcher(arguments, instanceFactory, inputParserProvider,
                computeEngine, storage, executorService);

        dispatcher.runBatch();
        verify(executorService, times(2)).submit(Mockito.<Callable<?>>any());
    }
}