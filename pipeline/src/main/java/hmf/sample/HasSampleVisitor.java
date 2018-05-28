package hmf.sample;

public interface HasSampleVisitor {
    void visit(FlowCell cell);

    void visit(Lane lane);
}
