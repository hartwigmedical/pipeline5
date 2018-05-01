package hmf.bwa;

import org.immutables.value.Value;

@Value.Immutable
public interface BwaConfiguration {

    @Value.Parameter
    String getReferenceFile();

    @Value.Parameter
    String getIndexFile();

    static BwaConfiguration of(String referenceFile, String indexFile){
        return ImmutableBwaConfiguration.of(referenceFile, indexFile);
    }
}
