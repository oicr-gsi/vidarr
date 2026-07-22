package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import java.util.Objects;

public final class OncoAnalyserSubmission {
    private final String submissionId;
    private final String templateId;
    private final String sequencingPlatform;
    private final SequencingData tumorSample;
    private final SequencingData referenceSample;
    private final SequencingData tumorRnaSample;

    public OncoAnalyserSubmission(
            String submissionId,
            String templateId,
            String sequencingPlatform,
            SequencingData tumorSample,
            SequencingData referenceSample,
            SequencingData tumorRnaSample
    ) {
        this.submissionId = submissionId;
        this.templateId = templateId;
        this.sequencingPlatform = sequencingPlatform;
        this.tumorSample = tumorSample;
        this.referenceSample = referenceSample;
        this.tumorRnaSample = tumorRnaSample;
    }

    public String submissionId() {
        return submissionId;
    }

    public String templateId() {
        return templateId;
    }

    public String sequencingPlatform() {
        return sequencingPlatform;
    }

    public SequencingData tumorSample() {
        return tumorSample;
    }

    public SequencingData referenceSample() {
        return referenceSample;
    }

    public SequencingData tumorRnaSample() {
        return tumorRnaSample;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (OncoAnalyserSubmission) obj;
        return Objects.equals(this.submissionId, that.submissionId) &&
                Objects.equals(this.templateId, that.templateId) &&
                Objects.equals(this.sequencingPlatform, that.sequencingPlatform) &&
                Objects.equals(this.tumorSample, that.tumorSample) &&
                Objects.equals(this.referenceSample, that.referenceSample) &&
                Objects.equals(this.tumorRnaSample, that.tumorRnaSample);
    }

    @Override
    public int hashCode() {
        return Objects.hash(submissionId, templateId, sequencingPlatform, tumorSample, referenceSample, tumorRnaSample);
    }

    @Override
    public String toString() {
        return "OncoAnalyserSubmission[" +
                "submissionId=" + submissionId + ", " +
                "templateId=" + templateId + ", " +
                "sequencingPlatform=" + sequencingPlatform + ", " +
                "tumorSample=" + tumorSample + ", " +
                "referenceSample=" + referenceSample + ", " +
                "tumorRnaSample=" + tumorRnaSample + ']';
    }

}
