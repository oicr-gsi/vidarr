package ca.on.oicr.gsi.vidarr.server.dto;

import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.InputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.RuntimeProvisioner;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class ServerConfiguration {

  private Map<String, ConsumableResource> consumableResources = Collections.emptyMap();
  private String dbHost;
  private String dbName;
  private String dbPass;
  private int dbPort;
  private String dbUser;
  private Map<String, InputProvisioner<?>> inputProvisioners;
  private String name;
  private Map<String, String> otherServers;
  private Map<String, OutputProvisioner<?, ?>> outputProvisioners;
  private int port = 8080;
  private Map<String, RuntimeProvisioner<?>> runtimeProvisioners;
  private Map<String, TargetConfiguration> targets;
  private String unloadDirectory = ".";
  private String url;
  private Map<String, WorkflowEngine<?, ?>> workflowEngines;

  public Map<String, ConsumableResource> getConsumableResources() {
    return consumableResources;
  }

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

  public Map<String, InputProvisioner<?>> getInputProvisioners() {
    return inputProvisioners;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getOtherServers() {
    return otherServers;
  }

  public Map<String, OutputProvisioner<?, ?>> getOutputProvisioners() {
    return outputProvisioners;
  }

  public int getPort() {
    return port;
  }

  public Map<String, RuntimeProvisioner<?>> getRuntimeProvisioners() {
    return runtimeProvisioners;
  }

  public Map<String, TargetConfiguration> getTargets() {
    return targets;
  }

  public String getUnloadDirectory() {
    return unloadDirectory;
  }

  public String getUrl() {
    return url;
  }

  public Map<String, WorkflowEngine<?, ?>> getWorkflowEngines() {
    return workflowEngines;
  }

  public void setConsumableResources(Map<String, ConsumableResource> consumableResources) {
    this.consumableResources = consumableResources;
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

  public void setInputProvisioners(Map<String, InputProvisioner<?>> inputProvisioners) {
    this.inputProvisioners = inputProvisioners;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setOtherServers(Map<String, String> otherServers) {
    this.otherServers = otherServers;
  }

  public void setOutputProvisioners(Map<String, OutputProvisioner<?, ?>> outputProvisioners) {
    this.outputProvisioners = outputProvisioners;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setRuntimeProvisioners(Map<String, RuntimeProvisioner<?>> runtimeProvisioners) {
    this.runtimeProvisioners = runtimeProvisioners;
  }

  public void setTargets(Map<String, TargetConfiguration> targets) {
    this.targets = targets;
  }

  public void setUnloadDirectory(String unloadDirectory) {
    this.unloadDirectory = unloadDirectory;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setWorkflowEngines(Map<String, WorkflowEngine<?, ?>> workflowEngines) {
    this.workflowEngines = workflowEngines;
  }
}
