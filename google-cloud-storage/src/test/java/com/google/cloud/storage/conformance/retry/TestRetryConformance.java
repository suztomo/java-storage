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

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertNotNull;

import com.google.auth.ServiceAccountSigner;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.conformance.storage.v1.InstructionList;
import com.google.cloud.conformance.storage.v1.Method;
import com.google.common.base.Joiner;
import com.google.errorprone.annotations.Immutable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

@Immutable
final class TestRetryConformance {

  private final String bucketName = "test-bucket";
  private final String bucketName2 = "test-bucket-2";
  private final String userProject = "user-project";
  private final String objectName = "objectName";

  private final byte[] helloWorldUtf8Bytes = "Hello, World!!!".getBytes(StandardCharsets.UTF_8);
  private final Path helloWorldFilePath = resolvePathForResource();
  private final ServiceAccountCredentials serviceAccountCredentials =
      resolveServiceAccountCredentials();

  private final String host;

  private final int scenarioId;
  private final Method method;
  private final InstructionList instruction;
  private final boolean preconditionsProvided;
  private final boolean expectSuccess;
  private final int mappingId;

  TestRetryConformance(
      String host,
      int scenarioId,
      Method method,
      InstructionList instruction,
      boolean preconditionsProvided,
      boolean expectSuccess) {
    this(host, scenarioId, method, instruction, preconditionsProvided, expectSuccess, 0);
  }

  TestRetryConformance(
      String host,
      int scenarioId,
      Method method,
      InstructionList instruction,
      boolean preconditionsProvided,
      boolean expectSuccess,
      int mappingId) {
    this.host = host;
    this.scenarioId = scenarioId;
    this.method = requireNonNull(method, "method must be non null");
    this.instruction = requireNonNull(instruction, "instruction must be non null");
    this.preconditionsProvided = preconditionsProvided;
    this.expectSuccess = expectSuccess;
    this.mappingId = mappingId;
  }

  public String getHost() {
    return host;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getBucketName2() {
    return bucketName2;
  }

  public String getUserProject() {
    return userProject;
  }

  public String getObjectName() {
    return objectName;
  }

  public byte[] getHelloWorldUtf8Bytes() {
    return helloWorldUtf8Bytes;
  }

  public Path getHelloWorldFilePath() {
    return helloWorldFilePath;
  }

  public int getScenarioId() {
    return scenarioId;
  }

  public Method getMethod() {
    return method;
  }

  public InstructionList getInstruction() {
    return instruction;
  }

  public boolean isPreconditionsProvided() {
    return preconditionsProvided;
  }

  public boolean isExpectSuccess() {
    return expectSuccess;
  }

  public int getMappingId() {
    return mappingId;
  }

  public ServiceAccountSigner getServiceAccountSigner() {
    return serviceAccountCredentials;
  }

  public String getTestName() {
    String instructionsDesc = Joiner.on("_").join(instruction.getInstructionsList());
    return String.format(
        "TestRetryConformance/%d-[%s]-%s-%d",
        scenarioId, instructionsDesc, method.getName(), mappingId);
  }

  @Override
  public String toString() {
    return getTestName();
  }

  private static Path resolvePathForResource() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    URL url = cl.getResource("com/google/cloud/storage/conformance/retry/hello-world.txt");
    assertNotNull(url);
    try {
      return Paths.get(url.toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private static ServiceAccountCredentials resolveServiceAccountCredentials() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    InputStream inputStream =
        cl.getResourceAsStream(
            "com/google/cloud/conformance/storage/v1/test_service_account.not-a-test.json");
    assertNotNull(inputStream);
    try {
      return ServiceAccountCredentials.fromStream(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
