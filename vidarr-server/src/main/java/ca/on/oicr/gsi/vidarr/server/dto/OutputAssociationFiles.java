package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class OutputAssociationFiles extends OutputAssociation {
  private Map<String, String> attributes = Collections.emptyMap();
  private List<SignedFile> files;
  private String metatype;

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public List<SignedFile> getFiles() {
    return files;
  }

  public String getMetatype() {
    return metatype;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public void setFiles(List<SignedFile> files) {
    this.files = files;
  }

  public void setMetatype(String metatype) {
    this.metatype = metatype;
  }
}
