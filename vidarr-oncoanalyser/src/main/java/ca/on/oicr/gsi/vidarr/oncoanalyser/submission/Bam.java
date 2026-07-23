package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

public class Bam extends SequencingData {
    private String path, indexPath;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getIndexPath() {
        return indexPath;
    }

    public void setIndexPath(String indexPath) {
        this.indexPath = indexPath;
    }
}
