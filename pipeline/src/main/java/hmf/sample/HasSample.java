package hmf.sample;

public interface HasSample {

    Sample sample();

    void accept(HasSampleVisitor visitor);
}
