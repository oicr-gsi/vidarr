package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class OutputAssociationRoot extends OutputAssociation {
  private Map<String, String> attributes = Collections.emptyMap();
  private List<SignedLimsKey> limsKeys;
  private String metatype;

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public List<SignedLimsKey> getLimsKeys() {
    return limsKeys;
  }

  public String getMetatype() {
    return metatype;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public void setLimsKeys(List<SignedLimsKey> limsKeys) {
    this.limsKeys = limsKeys;
  }

  public void setMetatype(String metatype) {
    this.metatype = metatype;
  }
}
