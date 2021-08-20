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

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

interface RpcMethod {

  /**
   * Enumerate the hierarchy of storage rpc methods.
   *
   * <p>These class names intentionally do not follow java convention, because they are mapping
   * directly to lower level values.
   */
  final class storage {
    enum bucket_acl implements RpcMethod {
      delete,
      get,
      insert,
      list,
      patch,
      update
    }

    enum buckets implements RpcMethod {
      delete,
      get,
      insert,
      list,
      patch,
      update,
      getIamPolicy,
      lockRetentionPolicy,
      setIamPolicy,
      testIamPermission
    }

    enum default_object_acl implements RpcMethod {
      delete,
      get,
      insert,
      list,
      patch,
      update
    }

    enum hmacKey implements RpcMethod {
      delete,
      get,
      list,
      update,
      create
    }

    enum notification implements RpcMethod {
      delete,
      get,
      insert,
      list
    }

    enum object_acl implements RpcMethod {
      delete,
      get,
      insert,
      list,
      patch,
      update
    }

    enum objects implements RpcMethod {
      delete,
      get,
      insert,
      list,
      patch,
      update,
      compose,
      rewrite,
      copy
    }

    enum serviceaccount implements RpcMethod {
      get,
      put
    }

    // create a map, which can be used to do a reverse lookup of an RpcMethod by its associated
    // string value.
    static final Map<String, RpcMethod> lookup =
        Stream.of(
                Arrays.stream(bucket_acl.values()),
                Arrays.stream(buckets.values()),
                Arrays.stream(default_object_acl.values()),
                Arrays.stream(hmacKey.values()),
                Arrays.stream(notification.values()),
                Arrays.stream(object_acl.values()),
                Arrays.stream(objects.values()),
                Arrays.stream(serviceaccount.values()))
            .flatMap(Function.identity()) // .flatten()
            .collect(
                Collectors.toMap(
                    m -> String.format("storage.%s.%s", m.getClass().getSimpleName(), m.name()),
                    Function.identity()));
  }
}
