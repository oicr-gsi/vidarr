package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.api.UnloadFilterAnalysisId;
import ca.on.oicr.gsi.vidarr.api.UnloadFilterAnd;
import ca.on.oicr.gsi.vidarr.api.UnloadFilterCompletedAfter;
import ca.on.oicr.gsi.vidarr.api.UnloadFilterExternalId;
import ca.on.oicr.gsi.vidarr.api.UnloadFilterExternalProvider;
import ca.on.oicr.gsi.vidarr.api.UnloadFilterLastSubmittedAfter;
import ca.on.oicr.gsi.vidarr.api.UnloadFilterNot;
import ca.on.oicr.gsi.vidarr.api.UnloadFilterOr;
import ca.on.oicr.gsi.vidarr.api.UnloadFilterWorkflowId;
import ca.on.oicr.gsi.vidarr.api.UnloadFilterWorkflowName;
import ca.on.oicr.gsi.vidarr.api.UnloadFilterWorkflowRunId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Defines a JSON object for unloading records
 *
 * <p>Since Vidarr knows about external systems, it is desirable for those external systems to be
 * used as a way to select the records that should be included.
 */
@JsonTypeIdResolver(UnloadFilter.UnloadFilterIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface UnloadFilter {
  /**
   * A mechanism to build the filter from primitive.
   *
   * @param <T> the type of the filter being constructed
   */
  interface Visitor<T> {

    /**
     * Create a match for workflow runs that produced certain files or URLS, by their Vidarr IDs.
     *
     * @param ids the analysis identifiers to match
     */
    T analysisId(Stream<String> ids);

    /**
     * Require a match of all the clauses provided.
     *
     * @param clauses the filters to include
     */
    T and(Stream<T> clauses);

    /**
     * Requires that the completion time of the workflow as submitted is after the specified
     * timestamp
     *
     * @param time the timestamp to check
     */
    T completedAfter(Instant time);

    /**
     * Create a match for workflow runs that include an external key with any of the providers
     * listed.
     *
     * @param providers the providers to match
     */
    T externalKey(Stream<String> providers);

    /**
     * Create a match for workflow runs that include an external key with the provider listed and
     * any of the identifiers provided.
     *
     * @param provider the external provider to match
     * @param ids the external identifiers to match
     */
    T externalKey(String provider, Stream<String> ids);

    /**
     * Requires that the last time the workflow as submitted is after the specified timestamp
     *
     * @param time the timestamp to check
     */
    T lastSubmittedAfter(Instant time);

    /**
     * Invert the result of a filter.
     *
     * @param clause the filter to invert
     */
    T not(T clause);

    /**
     * A filter which is constantly one value
     *
     * @param value the value the filter should return
     */
    T of(boolean value);

    /**
     * Require a match of any the clauses provided.
     *
     * @param clauses the filters to include
     */
    T or(Stream<T> clauses);

    /**
     * Match any of the workflow Vidarr identifiers (versions) provided.
     *
     * @param ids the workflow identifiers to match
     */
    T workflowId(Stream<String> ids);

    /**
     * Match any of the workflow names provided.
     *
     * @param names the human-readable workflow names to match
     */
    T workflowName(Stream<String> names);

    /**
     * Match any of the workflow run Vidarr identifiers provided.
     *
     * @param ids the workflow identifiers to match
     */
    T workflowRunId(Stream<String> ids);
  }

  final class UnloadFilterIdResolver extends TypeIdResolverBase {
    private final Map<String, Class<? extends UnloadFilter>> knownIds =
        Stream.concat(
                Stream.of(
                    new Pair<>("and", UnloadFilterAnd.class),
                    new Pair<>("not", UnloadFilterNot.class),
                    new Pair<>("or", UnloadFilterOr.class),
                    new Pair<>("vidarr-analysis-id", UnloadFilterAnalysisId.class),
                    new Pair<>("vidarr-completed-after", UnloadFilterCompletedAfter.class),
                    new Pair<>("vidarr-external-id", UnloadFilterExternalId.class),
                    new Pair<>("vidarr-external-provider", UnloadFilterExternalProvider.class),
                    new Pair<>("vidarr-last-submitted-after", UnloadFilterLastSubmittedAfter.class),
                    new Pair<>("vidarr-workflow-id", UnloadFilterWorkflowId.class),
                    new Pair<>("vidarr-workflow-name", UnloadFilterWorkflowName.class),
                    new Pair<>("vidarr-workflow-run-id", UnloadFilterWorkflowRunId.class)),
                ServiceLoader.load(UnloadFilterProvider.class).stream()
                    .map(Provider::get)
                    .flatMap(UnloadFilterProvider::types))
            .collect(Collectors.toMap(Pair::first, Pair::second));

    @Override
    public Id getMechanism() {
      return Id.CUSTOM;
    }

    @Override
    public String idFromValue(Object o) {
      return knownIds.entrySet().stream()
          .filter(known -> known.getValue().isInstance(o))
          .map(Entry::getKey)
          .findFirst()
          .orElseThrow();
    }

    @Override
    public String idFromValueAndType(Object o, Class<?> aClass) {
      return idFromValue(o);
    }

    @Override
    public JavaType typeFromId(DatabindContext context, String id) throws IOException {
      final var clazz = knownIds.get(id);
      return clazz == null ? null : context.constructType(clazz);
    }
  }

  final class UnloadFilterResolver extends StdTypeResolverBuilder {
    @Override
    public TypeDeserializer buildTypeDeserializer(
        DeserializationConfig config, JavaType baseType, Collection<NamedType> subtypes) {
      return useForType(baseType) ? super.buildTypeDeserializer(config, baseType, subtypes) : null;
    }

    @Override
    public TypeSerializer buildTypeSerializer(
        SerializationConfig config, JavaType baseType, Collection<NamedType> subtypes) {
      return useForType(baseType) ? super.buildTypeSerializer(config, baseType, subtypes) : null;
    }

    public boolean useForType(JavaType t) {
      return t.isJavaLangObject();
    }
  }

  /**
   * Build an equivalent filter out of primitive components
   *
   * @param visitor the primitives to use
   * @param <T> the resulting filter type
   * @return the resulting filter
   */
  <T> T convert(Visitor<T> visitor);
}
