package ca.on.oicr.gsi.vidarr.server;

import ca.on.oicr.gsi.status.ConfigurationSection;
import ca.on.oicr.gsi.status.Header;
import ca.on.oicr.gsi.status.NavigationMenu;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.status.ServerConfig;
import ca.on.oicr.gsi.status.StatusPage;
import ca.on.oicr.gsi.vidarr.server.dto.PluginServerConfiguration;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;

abstract class BasePluginServer<P, T> implements ServerConfig {
  protected final T instance;
  private final StatusPage status =
      new StatusPage(this) {
        @Override
        protected void emitCore(SectionRenderer sectionRenderer) throws XMLStreamException {
          // Do nothing.
        }

        @Override
        public Stream<ConfigurationSection> sections() {
          return Stream.of(
              new ConfigurationSection("Plugin") {
                @Override
                public void emit(SectionRenderer sectionRenderer) throws XMLStreamException {
                  configuration(sectionRenderer);
                }
              });
        }
      };

  public BasePluginServer(Class<P> pluginClass, String[] args) throws IOException {
    final var configuration =
        Server.MAPPER.readValue(new File(args[0]), PluginServerConfiguration.class);
    if (!configuration.getPlugin().has("type")) {
      throw new IllegalArgumentException("Plugin lacks type");
    }
    final var type = configuration.getPlugin().get("type").asText();
    instance =
        ServiceLoader.load(pluginClass).stream()
            .map(ServiceLoader.Provider::get)
            .filter(p -> typeFor(p).equals(type))
            .findAny()
            .map(p -> readConfiguration(p, configuration.getPlugin()))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(String.format("Unknown plugin type “%s”", type)));

    final var undertow =
        Undertow.builder()
            .addHttpListener(configuration.getPort(), "0.0.0.0")
            .setWorkerThreads(10 * Runtime.getRuntime().availableProcessors())
            .setHandler(
                handlers(Handlers.routing().get("/", this::status).get("/metrics", this::metrics)))
            .build();
    undertow.start();
  }

  protected abstract void configuration(SectionRenderer sectionRenderer) throws XMLStreamException;

  protected abstract HttpHandler handlers(RoutingHandler routingHandler);

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
    return "Víðarr Plugin";
  }

  @Override
  public Stream<NavigationMenu> navigation() {
    return Stream.empty();
  }

  protected abstract T readConfiguration(P provider, ObjectNode configuration);

  private void status(HttpServerExchange httpServerExchange) {
    httpServerExchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/html");
    httpServerExchange.setStatusCode(StatusCodes.OK);
    status.renderPage(httpServerExchange.getOutputStream());
  }

  protected abstract String typeFor(P pluginProvider);
}
