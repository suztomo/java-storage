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

final class Functions {

  @FunctionalInterface
  interface CtxFunction {

    Ctx apply(Ctx ctx, TestRetryConformance trc) throws Throwable;

    default CtxFunction andThen(CtxFunction f) {
      return (Ctx ctx, TestRetryConformance trc) -> f.apply(apply(ctx, trc), trc);
    }

    static CtxFunction identity() {
      return (ctx, c) -> ctx;
    }
  }

  /**
   * Define a Function which can throw, this simplifies the code where a checked exception is
   * declared. These Functions only exist in the context of tests so if a throw happens it will be
   * handled at a per-test level.
   */
  @FunctionalInterface
  public interface EFunction<A, B> {
    B apply(A a) throws Throwable;
  }

  /**
   * Define a Consumer which can throw, this simplifies the code where a checked exception is
   * declared. These Consumers only exist in the context of tests so if a throw happens it will be
   * handled at a per-test level.
   */
  @FunctionalInterface
  public interface EConsumer<A> {
    void consume(A a) throws Throwable;
  }

  @FunctionalInterface
  public interface VoidFunction {
    void apply();
  }
}
