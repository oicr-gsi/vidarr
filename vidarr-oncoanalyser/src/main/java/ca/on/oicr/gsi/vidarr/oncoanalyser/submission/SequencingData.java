package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import java.util.Set;

public class SequencingData {
    private String name;
    private Set<Sample> sequencingData;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<Sample> getSequencingData() {
        return sequencingData;
    }

    public void setSequencingData(Set<Sample> sequencingData) {
        this.sequencingData = sequencingData;
    }
}
