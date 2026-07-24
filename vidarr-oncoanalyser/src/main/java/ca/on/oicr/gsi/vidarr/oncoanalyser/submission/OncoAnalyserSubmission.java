package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

import java.util.Objects;

public record OncoAnalyserSubmission(
        String submissionId,
        String templateId,
        MolecularData molecularData) {
    public OncoAnalyserSubmission {
        Objects.requireNonNull(templateId);
        Objects.requireNonNull(molecularData);
    }
}
