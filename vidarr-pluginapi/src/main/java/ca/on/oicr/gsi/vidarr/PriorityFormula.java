package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.PriorityFormula.PriorityFormulaIdResolver;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import tools.jackson.databind.DatabindContext;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.annotation.JsonTypeIdResolver;
import tools.jackson.databind.jsontype.impl.TypeIdResolverBase;

/** Computes a new score from the input scores to determine a workflow run's priority */
@JsonTypeIdResolver(PriorityFormulaIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface PriorityFormula {

  final class PriorityFormulaIdResolver extends TypeIdResolverBase {

    private final Map<String, Class<? extends PriorityFormula>> knownIds =
        ServiceLoader.load(PriorityFormulaProvider.class).stream()
            .map(Provider::get)
            .flatMap(PriorityFormulaProvider::types)
            .collect(Collectors.toMap(Pair::first, Pair::second));

    @Override
    public Id getMechanism() {
      return Id.CUSTOM;
    }

    @Override
    public String idFromValue(DatabindContext context, Object value) {
      return knownIds.entrySet().stream()
          .filter(known -> known.getValue().isInstance(value))
          .map(Entry::getKey)
          .findFirst()
          .orElseThrow();
    }

    @Override
    public String idFromValueAndType(
        DatabindContext context, Object value, Class<?> suggestedType) {
      return idFromValue(context, value);
    }

    @Override
    public JavaType typeFromId(DatabindContext context, String id) {
      final var clazz = knownIds.get(id);
      return clazz == null ? null : context.constructType(clazz);
    }
  }

  /**
   * Calculate the appropriate score
   *
   * @param inputs the scores from the user-provided input
   * @param created the time when the workflow was first submitted
   * @return the score
   */
  int compute(ToIntFunction<String> inputs, Instant created);
}
