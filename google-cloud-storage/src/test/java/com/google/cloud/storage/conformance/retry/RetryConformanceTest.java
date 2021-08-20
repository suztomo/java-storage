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

import static com.google.cloud.storage.conformance.retry.Ctx.ctx;
import static com.google.cloud.storage.conformance.retry.State.empty;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.conformance.storage.v1.InstructionList;
import com.google.cloud.conformance.storage.v1.Method;
import com.google.cloud.conformance.storage.v1.RetryTest;
import com.google.cloud.conformance.storage.v1.RetryTests;
import com.google.cloud.storage.conformance.retry.Functions.CtxFunction;
import com.google.cloud.storage.conformance.retry.RetryTestFixture.CleanupStrategy;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.AssumptionViolatedException;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class RetryConformanceTest {
  private static final Logger LOGGER = Logger.getLogger(RetryConformanceTest.class.getName());

  private static final String HEX_SHUFFLE_SEED_OVERRIDE =
      System.getProperty("HEX_SHUFFLE_SEED_OVERRIDE");

  private static final Set<BiPredicate<RpcMethod, TestRetryConformance>> TEST_ALLOW_LIST =
      ImmutableSet.of((m, c) -> true);

  @ClassRule
  public static final TestBench TEST_BENCH =
      TestBench.newBuilder().setIgnorePullError(true).build();

  @Rule(order = 1)
  public final GracefulConformanceEnforcement gracefulConformanceEnforcement;

  @Rule(order = 2)
  public final RetryTestFixture retryTestFixture;

  private final TestRetryConformance testRetryConformance;
  private final RpcMethodMapping mapping;

  public RetryConformanceTest(TestRetryConformance testRetryConformance, RpcMethodMapping mapping) {
    this.testRetryConformance = testRetryConformance;
    this.mapping = mapping;
    this.gracefulConformanceEnforcement =
        new GracefulConformanceEnforcement(testRetryConformance.getTestName());
    this.retryTestFixture =
        new RetryTestFixture(CleanupStrategy.ONLY_ON_SUCCESS, TEST_BENCH, testRetryConformance);
  }

  @Test
  public void test() throws Throwable {
    Ctx ctx = ctx(retryTestFixture.getNonTestStorage(), empty());

    Ctx postSetupCtx =
        mapping
            .getSetup()
            .apply(ctx, testRetryConformance)
            .leftMap(s -> retryTestFixture.getTestStorage());

    Ctx postTestCtx =
        mapping
            .getTest()
            .apply(postSetupCtx, testRetryConformance)
            .leftMap(s -> retryTestFixture.getNonTestStorage());

    mapping.getTearDown().apply(postTestCtx, testRetryConformance);
  }

  /**
   * Load all of the tests and return a {@code Collection<Object[]>} representing the set of tests.
   * Each entry in the returned collection is the set of parameters to the constructor of this test
   * class.
   *
   * <p>The results of this method will then be run by JUnit's Parameterized test runner
   */
  @Parameters(name = "{0}")
  public static Collection<Object[]> testCases() throws IOException, NoSuchAlgorithmException {
    RetryTests retryTests = loadRetryTestsDefinition();

    List<RetryTest> retryTestCases =
        retryTests.getRetryTestsList().stream()
            .sorted(Comparator.comparingInt(RetryTest::getId))
            .collect(Collectors.toList());

    RpcMethodMappings rpcMethodMappings = new RpcMethodMappings();
    List<Object[]> testCases = generateTestCases(rpcMethodMappings, retryTestCases);

    Random rand = getRand();
    Collections.shuffle(testCases, rand);

    validateGeneratedTestCases(rpcMethodMappings, testCases);

    return testCases;
  }

  private static RetryTests loadRetryTestsDefinition() throws IOException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();

    String testDataJsonResource = "com/google/cloud/conformance/storage/v1/retry_tests.json";
    InputStream dataJson = cl.getResourceAsStream(testDataJsonResource);
    assertNotNull(
        String.format("Unable to load test definition: %s", testDataJsonResource), dataJson);

    InputStreamReader reader = new InputStreamReader(dataJson, Charsets.UTF_8);
    RetryTests.Builder testBuilder = RetryTests.newBuilder();
    JsonFormat.parser().merge(reader, testBuilder);
    return testBuilder.build();
  }

  private static List<Object[]> generateTestCases(
      RpcMethodMappings rpcMethodMappings, List<RetryTest> testCases) {
    String host = TEST_BENCH.getBaseUri().replaceAll("https?://", "");

    List<Object[]> data = new ArrayList<>(testCases.size());
    for (RetryTest testCase : testCases) {
      for (InstructionList instructionList : testCase.getCasesList()) {
        for (Method method : testCase.getMethodsList()) {
          String methodName = method.getName();
          RpcMethod key = RpcMethod.storage.lookup.get(methodName);
          assertNotNull(
              String.format("Unable to resolve RpcMethod for value '%s'", methodName), key);
          List<RpcMethodMapping> mappings =
              rpcMethodMappings.get(key).stream()
                  .sorted(Comparator.comparingInt(RpcMethodMapping::getMappingId))
                  .collect(Collectors.toList());
          if (mappings.isEmpty()) {
            TestRetryConformance testRetryConformance =
                new TestRetryConformance(
                    host,
                    testCase.getId(),
                    method,
                    instructionList,
                    testCase.getPreconditionProvided(),
                    false);
            if (TEST_ALLOW_LIST.stream().anyMatch(p -> p.test(key, testRetryConformance))) {
              data.add(new Object[] {testRetryConformance, RpcMethodMapping.notImplemented(key)});
            }
          } else {
            for (RpcMethodMapping mapping : mappings) {
              TestRetryConformance testRetryConformance =
                  new TestRetryConformance(
                      host,
                      testCase.getId(),
                      method,
                      instructionList,
                      testCase.getPreconditionProvided(),
                      testCase.getExpectSuccess(),
                      mapping.getMappingId());
              if (TEST_ALLOW_LIST.stream().anyMatch(p -> p.test(key, testRetryConformance))) {
                if (mapping.getApplicable().test(testRetryConformance)) {
                  data.add(new Object[] {testRetryConformance, mapping});
                } else {
                  RpcMethodMapping build =
                      mapping
                          .toBuilder()
                          .withSetup(CtxFunction.identity())
                          .withTest(
                              (s, c) -> {
                                throw new AssumptionViolatedException(
                                    "applicability predicate evaluated to false");
                              })
                          .withTearDown(CtxFunction.identity())
                          .build();
                  data.add(new Object[] {testRetryConformance, build});
                }
              }
            }
          }
        }
      }
    }
    return data;
  }

  private static Random getRand() throws NoSuchAlgorithmException {
    long seed;
    if (HEX_SHUFFLE_SEED_OVERRIDE != null) {
      LOGGER.info(
          "Shuffling test order using Random with override seed: " + HEX_SHUFFLE_SEED_OVERRIDE);
      // bytes = Base64.getDecoder().decode(BASE64_SHUFFLE_SEED_OVERRIDE);
      seed = new BigInteger(HEX_SHUFFLE_SEED_OVERRIDE.replace("0x", ""), 16).longValue();
    } else {
      // bytes = new byte[32];
      seed =
          SecureRandom.getInstanceStrong()
              .longs(100)
              .reduce((first, second) -> second)
              .orElseThrow(
                  () -> {
                    throw new IllegalStateException("Unable to generate seed");
                  });
      String msg = String.format("Shuffling test order using Random with seed: 0x%016X", seed);
      LOGGER.info(msg);
    }
    return new Random(seed);
  }

  private static void validateGeneratedTestCases(
      RpcMethodMappings rpcMethodMappings, List<Object[]> data) {
    Set<Integer> unusedMappings =
        rpcMethodMappings.differenceMappingIds(
            data.stream()
                .map(a -> ((TestRetryConformance) a[0]).getMappingId())
                .collect(Collectors.toSet()));

    if (!unusedMappings.isEmpty()) {
      LOGGER.warning(
          String.format(
              "Declared but unused mappings with ids: [%s]", Joiner.on(", ").join(unusedMappings)));
    }
  }
}
