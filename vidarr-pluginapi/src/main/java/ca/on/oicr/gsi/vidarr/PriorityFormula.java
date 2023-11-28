package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.PriorityFormula.PriorityFormulaIdResolver;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

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

  /**
   * Calculate the appropriate score
   *
   * @param inputs the scores from the user-provided input
   * @param created the time when the workflow was first submitted
   * @return the score
   */
  int compute(ToIntFunction<String> inputs, Instant created);
}
