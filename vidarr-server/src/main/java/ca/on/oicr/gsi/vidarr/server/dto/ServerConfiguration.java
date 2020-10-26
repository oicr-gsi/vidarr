package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class ServerConfiguration {
  private String dbHost;
  private String dbName;
  private String dbPass;
  private int dbPort;
  private String dbUser;
  private Map<String, String> otherServers;
  private int port = 8080;
  private Map<String, ObjectNode> outputProvisioners;
  private Map<String, ObjectNode> inputProvisioners;
  private Map<String, ObjectNode> runtimeProvisioners;
  private String url;
  private String name;
  private Map<String, ObjectNode> workflowEngines;
  private Map<String, TargetConfiguration> targets;

  public String getDbHost() {
    return dbHost;
  }

  public String getDbName() {
    return dbName;
  }

  public String getDbPass() {
    return dbPass;
  }

  public int getDbPort() {
    return dbPort;
  }

  public String getDbUser() {
    return dbUser;
  }

  public Map<String, ObjectNode> getInputProvisioners() {
    return inputProvisioners;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getOtherServers() {
    return otherServers;
  }

  public int getPort() {
    return port;
  }

  public Map<String, ObjectNode> getOutputProvisioners() {
    return outputProvisioners;
  }

  public Map<String, ObjectNode> getRuntimeProvisioners() {
    return runtimeProvisioners;
  }

  public Map<String, TargetConfiguration> getTargets() {
    return targets;
  }

  public String getUrl() {
    return url;
  }

  public Map<String, ObjectNode> getWorkflowEngines() {
    return workflowEngines;
  }

  public void setDbHost(String dbHost) {
    this.dbHost = dbHost;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setDbPass(String dbPass) {
    this.dbPass = dbPass;
  }

  public void setDbPort(int dbPort) {
    this.dbPort = dbPort;
  }

  public void setDbUser(String dbUser) {
    this.dbUser = dbUser;
  }

  public void setInputProvisioners(Map<String, ObjectNode> inputProvisioners) {
    this.inputProvisioners = inputProvisioners;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setOtherServers(Map<String, String> otherServers) {
    this.otherServers = otherServers;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setOutputProvisioners(Map<String, ObjectNode> outputProvisioners) {
    this.outputProvisioners = outputProvisioners;
  }

  public void setRuntimeProvisioners(Map<String, ObjectNode> runtimeProvisioners) {
    this.runtimeProvisioners = runtimeProvisioners;
  }

  public void setTargets(Map<String, TargetConfiguration> targets) {
    this.targets = targets;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setWorkflowEngines(Map<String, ObjectNode> workflowEngines) {
    this.workflowEngines = workflowEngines;
  }
}
