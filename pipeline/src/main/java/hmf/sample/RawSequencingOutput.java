package hmf.sample;

import java.io.File;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.immutables.value.Value;

import hmf.pipeline.Configuration;

@Value.Immutable
public interface RawSequencingOutput {

    String FIRST_IN_PAIR = "_R1";
    String LANE_PREFIX = "L0";

    FlowCell sampled();

    static RawSequencingOutput from(Configuration configuration) {
        ImmutableRawSequencingOutput.Builder builder = ImmutableRawSequencingOutput.builder();
        Collection<Lane> laneFiles = FileUtils.listFiles(new File(configuration.sampleDirectory()),
                filter(configuration.sampleName(), configuration.useInterleaved()),
                null)
                .stream()
                .map(File::getName)
                .map(RawSequencingOutput::indexFromFileName)
                .map(Integer::parseInt)
                .map(index -> Lane.of(Sample.of(configuration.sampleDirectory(), configuration.sampleName()), index))
                .collect(Collectors.toList());
        FlowCell real = FlowCell.builder().addAllLanes(laneFiles).build();
        return builder.sampled(real).build();
    }

    static String indexFromFileName(final String name) {
        return name.substring(name.indexOf(LANE_PREFIX) + 1, name.indexOf(LANE_PREFIX) + 4);
    }

    static IOFileFilter filter(final String sampleName, final boolean useInterleaved) {
        return new IOFileFilter() {
            @Override
            public boolean accept(final File file) {
                return file.getName().contains(sampleName) && (useInterleaved
                        ? file.getName().contains("interleaved")
                        : file.getName().contains(FIRST_IN_PAIR));
            }

            @Override
            public boolean accept(final File file, final String s) {
                return accept(file);
            }
        };
    }
}
