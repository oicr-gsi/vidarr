package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

public class FastqPair {
    private String lane, library, laneR1Path, laneR2Path;
    private ReadGroup readGroup;

    public String getLaneR1Path() {
        return laneR1Path;
    }

    public void setLaneR1Path(String laneR1Path) {
        this.laneR1Path = laneR1Path;
    }

    public String getLaneR2Path() {
        return laneR2Path;
    }

    public void setLaneR2Path(String laneR2Path) {
        this.laneR2Path = laneR2Path;
    }

    public String getLane() {
        return lane;
    }

    public void setLane(String lane) {
        this.lane = lane;
    }

    public String getLibrary() {
        return library;
    }

    public void setLibrary(String library) {
        this.library = library;
    }

    public ReadGroup getReadGroup() {
        return readGroup;
    }

    public void setReadGroup(ReadGroup readGroup) {
        this.readGroup = readGroup;
    }
}
