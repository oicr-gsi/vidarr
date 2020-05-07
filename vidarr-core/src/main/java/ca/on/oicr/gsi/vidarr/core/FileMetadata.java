package ca.on.oicr.gsi.vidarr.core;

import java.util.stream.Stream;

/** Get file metadata for a Vidarr file that can be used as input */
public interface FileMetadata {
  /** The permanent storage path for this file */
  String path();

  /** The external keys associated with this file */
  Stream<ExternalKey> externalKeys();
}
