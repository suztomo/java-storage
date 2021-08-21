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

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.conformance.retry.Functions.EConsumer;
import com.google.cloud.storage.conformance.retry.Functions.EFunction;

final class Ctx {

  private final Storage storage;
  private final State state;

  public Ctx(Storage s, State t) {
    this.storage = s;
    this.state = t;
  }

  static <S extends State> Ctx ctx(Storage storage, S state) {
    return new Ctx(storage, state);
  }

  public Storage getStorage() {
    return storage;
  }

  public State getState() {
    return state;
  }

  public Ctx leftMap(EFunction<Storage, Storage> f) throws Throwable {
    return new Ctx(f.apply(storage), state);
  }

  public Ctx map(EFunction<State, State> f) throws Throwable {
    return new Ctx(storage, f.apply(state));
  }

  public Ctx peek(EConsumer<State> f) throws Throwable {
    f.consume(state);
    return this;
  }
}
