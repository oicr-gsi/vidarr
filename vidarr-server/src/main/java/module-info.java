import ca.on.oicr.gsi.vidarr.ConsumableResourceProvider;
import ca.on.oicr.gsi.vidarr.InputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.RuntimeProvisionerProvider;
import ca.on.oicr.gsi.vidarr.UnloadFilterProvider;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;

module ca.on.oicr.gsi.vidarr.server {
  exports ca.on.oicr.gsi.vidarr.server;

  requires ca.on.oicr.gsi.serverutils;
  requires ca.on.oicr.gsi.vidarr.core;
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires com.fasterxml.jackson.datatype.jsr310;
  requires HikariCP;
  requires java.management;
  requires java.naming;
  requires java.net.http;
  requires java.sql;
  requires java.xml;
  requires jdk.unsupported;
  requires org.flywaydb.core;
  requires org.jooq.codegen;
  requires org.jooq.meta;
  requires org.jooq;
  requires org.postgresql.jdbc;
  requires simpleclient.common;
  requires simpleclient.hotspot;
  requires simpleclient;
  requires undertow.core;

  opens ca.on.oicr.gsi.vidarr.server.dto to
      com.fasterxml.jackson.annotation,
      com.fasterxml.jackson.core,
      com.fasterxml.jackson.databind;
  opens ca.on.oicr.gsi.vidarr.server.jooq.tables.records to
      org.jooq;
  opens db.migration;

  uses ConsumableResourceProvider;
  uses InputProvisionerProvider;
  uses OutputProvisionerProvider;
  uses RuntimeProvisionerProvider;
  uses UnloadFilterProvider;
  uses WorkflowEngineProvider;
}
