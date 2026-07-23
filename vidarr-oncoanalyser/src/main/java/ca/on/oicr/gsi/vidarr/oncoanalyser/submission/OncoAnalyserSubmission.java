package ca.on.oicr.gsi.vidarr.oncoanalyser.submission;

public record OncoAnalyserSubmission(
        String submissionId,
        String templateId,
        MolecularData molecularData) {}
