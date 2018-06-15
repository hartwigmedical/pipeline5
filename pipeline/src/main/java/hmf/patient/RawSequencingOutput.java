package hmf.patient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.immutables.value.Value;

import hmf.pipeline.Configuration;

@Value.Immutable
public interface RawSequencingOutput {

    enum TypePostfix {
        NORMAL("R"),
        TUMOUR("T");
        private final String postfix;

        TypePostfix(final String postfix) {
            this.postfix = postfix;
        }

        public String getPostfix() {
            return postfix;
        }
    }

    Patient patient();

    static RawSequencingOutput from(Configuration configuration) throws IOException {
        ImmutableRawSequencingOutput.Builder builder = ImmutableRawSequencingOutput.builder();
        Optional<Path> maybeNormalDirectory = findDirectoryByConvention(configuration, TypePostfix.NORMAL);
        Optional<Path> maybeTumourDirectory = findDirectoryByConvention(configuration, TypePostfix.TUMOUR);

        if (maybeNormalDirectory.isPresent() && maybeTumourDirectory.isPresent()) {
            return subdirectoriesForNormalAndTumour(configuration, builder, maybeNormalDirectory.get(), maybeTumourDirectory.get());
        } else {
            return normalAndTumourInSameDirectory(configuration, builder);
        }
    }

    static RawSequencingOutput normalAndTumourInSameDirectory(final Configuration configuration,
            final ImmutableRawSequencingOutput.Builder builder) throws IOException {
        return patientOf(configuration,
                builder,
                createPairedEndSample(Paths.get(configuration.patientDirectory()), configuration.patientName(), TypePostfix.NORMAL),
                createPairedEndSample(Paths.get(configuration.patientDirectory()), configuration.patientName(), TypePostfix.TUMOUR));
    }

    static RawSequencingOutput subdirectoriesForNormalAndTumour(final Configuration configuration,
            final ImmutableRawSequencingOutput.Builder builder, final Path normalDirectory, final Path tumourDirectory) throws IOException {
        return patientOf(configuration,
                builder,
                createPairedEndSample(normalDirectory, configuration.patientName(), TypePostfix.NORMAL),
                createPairedEndSample(tumourDirectory, configuration.patientName(), TypePostfix.TUMOUR));
    }

    static RawSequencingOutput patientOf(final Configuration configuration, final ImmutableRawSequencingOutput.Builder builder,
            final Sample normal, final Sample tumour) throws IOException {
        Patient patient = Patient.of(configuration.patientDirectory(), configuration.patientName(), normal, tumour);
        return builder.patient(patient).build();
    }

    static Optional<Path> findDirectoryByConvention(final Configuration configuration, final TypePostfix typePostfix) throws IOException {
        return StreamSupport.stream(Files.newDirectoryStream(Paths.get(configuration.patientDirectory()),
                configuration.patientName() + typePostfix.getPostfix()).spliterator(), false).findFirst();
    }

    static Sample createPairedEndSample(final Path sampleDirectory, final String sampleName, TypePostfix postfix) throws IOException {
        Map<String, ImmutableLane.Builder> builders = new HashMap<>();
        String sampleNameWithPostfix = sampleName + postfix.getPostfix();
        for (Path path : Files.newDirectoryStream(sampleDirectory, sampleNameWithPostfix + "_*_S?_L*_R?_*.fastq*")) {
            String[] tokens = path.toFile().getName().split("_");
            String laneName = tokens[3];
            ImmutableLane.Builder builder = builders.computeIfAbsent(laneName,
                    s -> Lane.builder().directory(sampleDirectory.toString()).name(sampleNameWithPostfix + "_" + s));
            if (tokens[4].equals("R1")) {
                builder.readsFile(path.toString());
            } else if (tokens[4].equals("R2")) {
                builder.matesFile(path.toString());
            }
        }
        return Sample.builder(sampleDirectory.toString(), sampleNameWithPostfix)
                .addAllLanes(builders.values().stream().map(ImmutableLane.Builder::build).collect(Collectors.toList()))
                .build();
    }
}
