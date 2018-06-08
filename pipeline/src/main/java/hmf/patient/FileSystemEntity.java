package hmf.patient;

public interface FileSystemEntity {

    String directory();

    void accept(FileSystemVisitor visitor);
}
