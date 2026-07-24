package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Objects;

public class Bam extends SequencingData {
    private String path, indexPath;

    public Bam(){}

    public Bam(String path, String indexPath){
        Objects.requireNonNull(path);
        this.path = path;
        this.indexPath = indexPath;
    }

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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Bam bam = (Bam) o;
        return Objects.equals(path, bam.path) && Objects.equals(indexPath, bam.indexPath);
    }

    @Override
    public String toString() {
        return "Bam{" +
                "path='" + path + '\'' +
                ", indexPath='" + indexPath + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, indexPath);
    }
}
