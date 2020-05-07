package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.ZonedDateTime;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SignedFile {
  private String file;
  private String id;
  private ZonedDateTime lastModified;
  private String provider;
  private ObjectNode signature;
  private String version;

  public String getFile() {
    return file;
  }

  public String getId() {
    return id;
  }

  public ZonedDateTime getLastModified() {
    return lastModified;
  }

  public String getProvider() {
    return provider;
  }

  public ObjectNode getSignature() {
    return signature;
  }

  public String getVersion() {
    return version;
  }

  public void setFile(String file) {
    this.file = file;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setLastModified(ZonedDateTime lastModified) {
    this.lastModified = lastModified;
  }

  public void setProvider(String provider) {
    this.provider = provider;
  }

  public void setSignature(ObjectNode signature) {
    this.signature = signature;
  }

  public void setVersion(String version) {
    this.version = version;
  }
}
