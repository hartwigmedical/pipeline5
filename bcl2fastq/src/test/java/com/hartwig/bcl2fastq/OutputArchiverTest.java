package com.hartwig.bcl2fastq;

import com.google.common.collect.ImmutableList;
import com.hartwig.bcl2fastq.conversion.Conversion;
import com.hartwig.bcl2fastq.conversion.ConvertedFastq;
import com.hartwig.bcl2fastq.conversion.ConvertedSample;
import com.hartwig.pipeline.storage.GsUtilFacade;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hartwig.pipeline.storage.GsUtilFacade.GsCopyOption.NO_CLOBBER;
import static java.lang.String.format;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class OutputArchiverTest {
    private Bcl2fastqArguments arguments;
    private GsUtilFacade gsUtil;
    private OutputArchiver victim;

    @Before
    public void setup() {
        arguments = arguments();
        gsUtil = mock(GsUtilFacade.class);
        victim = new OutputArchiver(arguments, gsUtil);
    }

    @Test
    public void shouldDoNothingOnEmptyConversion() {
        Conversion conversion = Conversion.builder().flowcell("flow").totalReads(0).undeterminedReads(0).build();
        victim.accept(conversion);
        verifyZeroInteractions(gsUtil);
    }

    @Test
    public void shouldArchiveBothOutputFilesFromEachConvertedFastq() {
        String fastqaOutputPathR1 = "fastqaOutputPathR1";
        String fastqaOutputPathR2 = "fastqaOutputPathR2";
        String fastqbOutputPathR1 = "fastqbOutputPathR1";
        String fastqbOutputPathR2 = "fastqbOutputPathR2";
        String fastqcOutputPathR1 = "fastqcOutputPathR1";
        String fastqcOutputPathR2 = "fastqcOutputPathR2";

        ConvertedFastq fastqA = mock(ConvertedFastq.class);
        when(fastqA.outputPathR1()).thenReturn(fastqaOutputPathR1);
        when(fastqA.outputPathR2()).thenReturn(fastqaOutputPathR2);
        ConvertedFastq fastqB = mock(ConvertedFastq.class);
        when(fastqB.outputPathR1()).thenReturn(fastqbOutputPathR1);
        when(fastqB.outputPathR2()).thenReturn(fastqbOutputPathR2);
        ConvertedFastq fastqC = mock(ConvertedFastq.class);
        when(fastqC.outputPathR1()).thenReturn(fastqcOutputPathR1);
        when(fastqC.outputPathR2()).thenReturn(fastqcOutputPathR2);

        Conversion conversion = mock(Conversion.class);
        ConvertedSample sampleA = mock(ConvertedSample.class);
        ConvertedSample sampleB = mock(ConvertedSample.class);
        when(conversion.samples()).thenReturn(ImmutableList.of(sampleA, sampleB));
        when(sampleA.fastq()).thenReturn(ImmutableList.of(fastqA, fastqB));
        when(sampleB.fastq()).thenReturn(ImmutableList.of(fastqC));

        victim.accept(conversion);

        verifyCopy(fastqaOutputPathR1);
        verifyCopy(fastqaOutputPathR2);
        verifyCopy(fastqbOutputPathR1);
        verifyCopy(fastqbOutputPathR2);
        verifyCopy(fastqcOutputPathR1);
        verifyCopy(fastqcOutputPathR2);

        System.out.println(arguments().toString());
    }

    private void verifyCopy(String source) {
        verify(gsUtil).copy(format("gs://%s/%s", arguments.outputBucket(), source),
                format("gs://%s", arguments.archiveBucket()), NO_CLOBBER);
    }

    private Bcl2fastqArguments arguments() {
        List<String> withValues = new ArrayList<>();
        ImmutableList.of(Bcl2fastqArguments.OUTPUT_BUCKET, Bcl2fastqArguments.INPUT_BUCKET,
                Bcl2fastqArguments.PRIVATE_KEY_PATH, Bcl2fastqArguments.SERVICE_ACCOUNT_EMAIL,
                Bcl2fastqArguments.FLOWCELL, Bcl2fastqArguments.SBP_API_URL, Bcl2fastqArguments.ARCHIVE_BUCKET,
                Bcl2fastqArguments.ARCHIVE_PRIVATE_KEY_PATH, Bcl2fastqArguments.ARCHIVE_PROJECT).forEach(s -> {
            withValues.add("-" + s);
            withValues.add("empty");
        });
        return Bcl2fastqArguments.from(withValues.toArray(new String[]{}));
    }
}