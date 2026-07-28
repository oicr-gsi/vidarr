package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Bam extends SequencingData {
    private String path, indexPath;

    @JsonCreator
    public Bam(@JsonProperty(value = "path", required = true) String path, @JsonProperty(value = "indexPath") String indexPath){
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
