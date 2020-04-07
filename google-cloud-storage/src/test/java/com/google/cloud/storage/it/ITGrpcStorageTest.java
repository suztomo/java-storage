/*
 * Copyright 2015 Google LLC
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

package com.google.cloud.storage.it;

import static org.junit.Assert.assertEquals;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import org.junit.BeforeClass;
import org.junit.Test;

public class ITGrpcStorageTest {

  private static Storage grpcStorage;
  private static Storage httpStorage;

  @BeforeClass
  public static void beforeClass() throws IOException {
    grpcStorage = StorageOptions.getDefaultGrpcInstance().getService();
    httpStorage = StorageOptions.getDefaultInstance().getService();
  }

  @Test
  public void testSimpleDownload() {
    String grpcBucketName = "gcs-grpc-team-franks-bucket";
    String grpcObjectName = "grpc-test-object";
    String objectContent = "random info blah blah";
    // Upload test object using JSON API
    httpStorage.create(
        BlobInfo.newBuilder(grpcBucketName, grpcObjectName).build(), objectContent.getBytes());

    // Download test object using gRPC API
    byte[] data = grpcStorage.readAllBytes(BlobId.of(grpcBucketName, grpcObjectName));
    assertEquals(new String(data), objectContent);
  }
}
