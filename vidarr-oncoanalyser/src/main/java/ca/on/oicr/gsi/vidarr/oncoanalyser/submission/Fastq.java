package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import java.util.Set;

public class Fastq extends Sample {
    private Set<FastqPair> pairs;

    public Set<FastqPair> getPairs() {
        return pairs;
    }

    public void setPairs(Set<FastqPair> pairs) {
        this.pairs = pairs;
    }
}
