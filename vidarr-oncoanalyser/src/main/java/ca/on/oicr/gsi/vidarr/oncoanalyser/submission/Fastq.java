package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import java.util.Objects;
import java.util.Set;

public class Fastq extends SequencingData {
    private Set<FastqPair> pairs;

    public Fastq(Set<FastqPair> pairs){
        Objects.requireNonNull(pairs);
        this.pairs = pairs;
    }

    public Set<FastqPair> getPairs() {
        return pairs;
    }

    @Override
    public String toString() {
        return "Fastq{" +
                "pairs=" + pairs +
                '}';
    }

    public void setPairs(Set<FastqPair> pairs) {
        this.pairs = pairs;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Fastq fastq = (Fastq) o;
        return Objects.equals(pairs, fastq.pairs);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(pairs);
    }
}
