/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.storage.conformance.retry;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Immutable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.OptionalInt;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@Immutable
final class RpcMethodMappings {
  private static final Logger LOGGER = Logger.getLogger(RpcMethodMappings.class.getName());

  final Multimap<RpcMethod, RpcMethodMapping> funcMap;

  RpcMethodMappings() {
    this(true);
  }

  private RpcMethodMappings(boolean failOnDuplicateIds) {
    ArrayList<RpcMethodMapping> a = new ArrayList<>();

    // TODO: fill in method mappings here

    validateMappingDefinitions(failOnDuplicateIds, a);

    funcMap = Multimaps.index(a, RpcMethodMapping::getMethod);
    reportMappingSummary();
  }

  public Collection<RpcMethodMapping> get(RpcMethod key) {
    return funcMap.get(key);
  }

  public Set<Integer> differenceMappingIds(Set<Integer> usedMappingIds) {
    return Sets.difference(
        funcMap.values().stream().map(RpcMethodMapping::getMappingId).collect(Collectors.toSet()),
        usedMappingIds);
  }

  private void validateMappingDefinitions(
      boolean failOnDuplicateIds, ArrayList<RpcMethodMapping> a) {
    ListMultimap<Integer, RpcMethodMapping> idMappings =
        MultimapBuilder.hashKeys()
            .arrayListValues()
            .build(Multimaps.index(a, RpcMethodMapping::getMappingId));
    String duplicateIds =
        idMappings.asMap().entrySet().stream()
            .filter(e -> e.getValue().size() > 1)
            .map(Entry::getKey)
            .map(i -> Integer.toString(i))
            .collect(Collectors.joining(", "));
    if (!duplicateIds.isEmpty()) {
      String message = "duplicate mapping ids present: [" + duplicateIds + "]";
      if (failOnDuplicateIds) {
        throw new IllegalStateException(message);
      } else {
        LOGGER.warning(message);
      }
    }
  }

  private void reportMappingSummary() {
    int mappingCount = funcMap.values().stream().mapToInt(m -> 1).sum();
    LOGGER.info("Current total number of mappings defined: " + mappingCount);
    String counts =
        funcMap.asMap().entrySet().stream()
            .map(
                e -> {
                  RpcMethod rpcMethod = e.getKey();
                  Collection<RpcMethodMapping> mapings = e.getValue();
                  return String.format(
                      "\t%s.%s: %d",
                      rpcMethod
                          .getClass()
                          .getName()
                          .replace("com.google.cloud.storage.conformance.retry.RpcMethod$", "")
                          .replace("$", "."),
                      rpcMethod,
                      mapings.size());
                })
            .sorted()
            .collect(Collectors.joining("\n", "\n", ""));
    LOGGER.info("Current number of mappings per rpc method: " + counts);
    OptionalInt max =
        funcMap.values().stream().map(RpcMethodMapping::getMappingId).mapToInt(i -> i).max();
    if (max.isPresent()) {
      LOGGER.info(String.format("Current max mapping index is: %d%n", max.getAsInt()));
    } else {
      throw new IllegalStateException("No mappings defined");
    }
  }
}
