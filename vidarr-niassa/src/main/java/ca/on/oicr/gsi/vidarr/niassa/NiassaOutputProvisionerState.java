package ca.on.oicr.gsi.vidarr.niassa;

import java.nio.file.Path;
import java.util.Map;

public class NiassaOutputProvisionerState {

    private final Map<String, String> labels;
    private final String md5;
    private final Path path;
    private final long filesize;

    public NiassaOutputProvisionerState(Map<String, String> labels, String md5, Path path, long filesize) {
        this.labels = labels;
        this.md5 = md5;
        this.path = path;
        this.filesize = filesize;
    }
}
