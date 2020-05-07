import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.UnloaderProvider;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;

module ca.on.oicr.gsi.vidarr.server {
  exports ca.on.oicr.gsi.vidarr.server;

  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires ca.on.oicr.gsi.vidarr.core;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires java.naming;
  requires java.sql;
  requires java.xml;
  requires org.jooq.codegen;
  requires org.jooq.meta;
  requires org.jooq;
  requires org.postgresql.jdbc;
  requires server.utils;
  requires simpleclient.common;
  requires simpleclient;
  requires undertow.core;

  opens ca.on.oicr.gsi.vidarr.server.dto to
      com.fasterxml.jackson.annotation,
      com.fasterxml.jackson.core,
      com.fasterxml.jackson.databind;

  uses OutputProvisionerProvider;
  uses UnloaderProvider;
  uses WorkflowEngineProvider;
}
