package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PluginServerConfiguration {

  private ObjectNode plugin;
  private int port = 8080;

  public ObjectNode getPlugin() {
    return plugin;
  }

  public int getPort() {
    return port;
  }

  public void setPlugin(ObjectNode plugin) {
    this.plugin = plugin;
  }

  public void setPort(int port) {
    this.port = port;
  }
}
