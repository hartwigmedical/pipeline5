package hmf.patient;

public interface FileSystemVisitor {

    void visit(Patient patient);

    void visit(Sample sample);

    void visit(Lane lane);

    default void deny(String reason) {
        throw new RuntimeException(reason);
    }
}
