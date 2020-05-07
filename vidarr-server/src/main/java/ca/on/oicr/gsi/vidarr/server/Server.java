package ca.on.oicr.gsi.vidarr.server;

import ca.on.oicr.gsi.status.ConfigurationSection;
import ca.on.oicr.gsi.status.Header;
import ca.on.oicr.gsi.status.NavigationMenu;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.status.ServerConfig;
import ca.on.oicr.gsi.status.StatusPage;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.server.dto.RoutingParameterType;
import ca.on.oicr.gsi.vidarr.server.dto.ServerConfiguration;
import ca.on.oicr.gsi.vidarr.server.remote.RemoteOutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.server.remote.RemoteWorkflowEngineProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;
import org.postgresql.ds.PGConnectionPoolDataSource;

public final class Server implements ServerConfig {

  static final ObjectMapper MAPPER = new ObjectMapper();

  @SafeVarargs
  private static <P, T> Map<String, T> load(
      String name,
      Class<P> clazz,
      Function<P, String> namer,
      BiFunction<P, ObjectNode, T> reader,
      Map<String, ObjectNode> configuration,
      P... fixedProviders) {
    final var providers =
        Stream.concat(
                ServiceLoader.load(clazz).stream().map(Provider::get), Stream.of(fixedProviders))
            .collect(Collectors.toMap(namer, Function.identity()));
    final var output = new TreeMap<String, T>();
    for (final var entry : configuration.entrySet()) {
      if (!entry.getValue().has("type")) {
        throw new IllegalArgumentException(
            String.format("%s record %s lacks type", name, entry.getKey()));
      }
      final var type = entry.getValue().get("type").asText();
      final var provider = providers.get(type);
      if (provider == null) {
        throw new IllegalArgumentException(
            String.format(
                "%s record %s has type %s, but this is not registered. Maybe a missing module?",
                name, entry.getKey(), type));
      }
      output.put(entry.getKey(), reader.apply(provider, entry.getValue()));
    }
    return output;
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println(
          "Usage: java --module-path MODULES --module ca.on.oicr.gsi.vidarr.server"
              + " configuration.json");
    }
    final var server = new Server(MAPPER.readValue(new File(args[0]), ServerConfiguration.class));
    final var undertow =
        Undertow.builder()
            .addHttpListener(server.port, "0.0.0.0")
            .setWorkerThreads(10 * Runtime.getRuntime().availableProcessors())
            .setHandler(
                Handlers.routing().get("/", server::status).get("/metrics", server::metrics))
            .build();
    undertow.start();
  }

  private final PGConnectionPoolDataSource dataSource;
  private final int port;
  private final Map<String, OutputProvisioner> provisioners;
  private final WorkflowRouter router;
  private final Map<String, RoutingParameterType> routingParameters;
  private final String selfUrl;
  private final Map<String, WorkflowEngine> workflowEngines;
  private final StatusPage status =
      new StatusPage(this) {
        @Override
        protected void emitCore(SectionRenderer sectionRenderer) throws XMLStreamException {
          sectionRenderer.line("Self-URL", selfUrl);
          router.display(sectionRenderer);
        }

        @Override
        public Stream<ConfigurationSection> sections() {
          return Stream.concat(
              workflowEngines.entrySet().stream()
                  .map(
                      e ->
                          new ConfigurationSection("Workflow Engine: " + e.getKey()) {
                            @Override
                            public void emit(SectionRenderer sectionRenderer)
                                throws XMLStreamException {
                              e.getValue().configuration(sectionRenderer);
                            }
                          }),
              provisioners.entrySet().stream()
                  .map(
                      e ->
                          new ConfigurationSection("Provisioner: " + e.getKey()) {
                            @Override
                            public void emit(SectionRenderer sectionRenderer)
                                throws XMLStreamException {
                              e.getValue().configuration(sectionRenderer);
                            }
                          }));
        }
      };

  private Server(ServerConfiguration configuration) {
    selfUrl = configuration.getUrl();
    port = configuration.getPort();
    routingParameters = configuration.getRouterParameters();
    workflowEngines =
        load(
            "Workflow engine",
            WorkflowEngineProvider.class,
            WorkflowEngineProvider::type,
            WorkflowEngineProvider::readConfiguration,
            configuration.getWorkflowEngines(),
            new RemoteWorkflowEngineProvider());
    provisioners =
        load(
            "Provisioner",
            OutputProvisionerProvider.class,
            OutputProvisionerProvider::type,
            OutputProvisionerProvider::readConfiguration,
            configuration.getProvisioners(),
            new RemoteOutputProvisionerProvider());
    var router = WorkflowRouter.DENY;
    for (var i = configuration.getRouting().size() - 1; i >= 0; i--) {
      router =
          configuration
              .getRouting()
              .get(i)
              .resolve(workflowEngines, provisioners, routingParameters, router);
    }
    this.router = router;
    dataSource = new PGConnectionPoolDataSource();
    dataSource.setServerNames(new String[] {configuration.getDbHost()});
    dataSource.setPortNumbers(new int[] {configuration.getDbPort()});
    dataSource.setDatabaseName(configuration.getDbName());
    dataSource.setUser(configuration.getDbUser());
    dataSource.setPassword(configuration.getDbPass());
  }

  @Override
  public Stream<Header> headers() {
    return Stream.empty();
  }

  private void metrics(HttpServerExchange httpServerExchange) {
    httpServerExchange.getResponseHeaders().put(Headers.CONTENT_TYPE, TextFormat.CONTENT_TYPE_004);
    httpServerExchange.setStatusCode(200);
    try (final var os = httpServerExchange.getOutputStream();
        final var writer = new PrintWriter(os)) {
      TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public String name() {
    return "Víðarr";
  }

  @Override
  public Stream<NavigationMenu> navigation() {
    return Stream.empty();
  }

  private void status(HttpServerExchange httpServerExchange) {
    httpServerExchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/html");
    httpServerExchange.setStatusCode(StatusCodes.OK);
    status.renderPage(httpServerExchange.getOutputStream());
  }
}
