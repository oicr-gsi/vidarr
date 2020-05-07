package ca.on.oicr.gsi.vidarr.core;

import java.util.Optional;

/** A service that can determine file information for previously created Vidarr files */
public interface FileResolver {
  /**
   * Get the file metadata for a Vidarr ID
   *
   * @param id the Vidarr ID
   * @return the file metadata, if it is available
   */
  Optional<FileMetadata> pathForId(String id);
}
