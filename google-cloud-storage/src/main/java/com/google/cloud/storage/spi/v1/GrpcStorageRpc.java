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

package com.google.cloud.storage.spi.v1;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;
import com.google.api.gax.core.GaxProperties;
import com.google.api.gax.rpc.ClientContext;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.gax.rpc.NoHeaderProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Get;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.BucketAccessControl;
import com.google.api.services.storage.model.HmacKey;
import com.google.api.services.storage.model.HmacKeyMetadata;
import com.google.api.services.storage.model.Notification;
import com.google.api.services.storage.model.ObjectAccessControl;
import com.google.api.services.storage.model.Policy;
import com.google.api.services.storage.model.ServiceAccount;
import com.google.api.services.storage.model.StorageObject;
import com.google.api.services.storage.model.TestIamPermissionsResponse;
import com.google.cloud.ServiceOptions;
import com.google.cloud.Tuple;
import com.google.cloud.google.storage.v1.StorageSettings;
import com.google.cloud.google.storage.v1.stub.GrpcStorageStub;
import com.google.cloud.google.storage.v1.stub.StorageStub;
import com.google.cloud.google.storage.v1.stub.StorageStubSettings;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.grpc.GrpcTransportOptions.ExecutorFactory;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.storage.v1.GetObjectMediaRequest;
import com.google.storage.v1.GetObjectMediaResponse;
import io.opencensus.trace.Span;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.NotImplementedException;

public class GrpcStorageRpc implements StorageRpc {
  public static final String DEFAULT_PROJECTION = "full";
  public static final String NO_ACL_PROJECTION = "noAcl";
  private static final String ENCRYPTION_KEY_PREFIX = "x-goog-encryption-";
  private static final String SOURCE_ENCRYPTION_KEY_PREFIX = "x-goog-copy-source-encryption-";

  // declare this HttpStatus code here as it's not included in java.net.HttpURLConnection
  private static final int SC_REQUESTED_RANGE_NOT_SATISFIABLE = 416;

  private final StorageStub storageStub;
  private final ScheduledExecutorService executor;
  private final ExecutorFactory<ScheduledExecutorService> executorFactory;
  private final ClientContext clientContext;

  private static final long MEGABYTE = 1024L * 1024L;

  public GrpcStorageRpc(StorageOptions options) {
    GrpcTransportOptions transportOptions = (GrpcTransportOptions) options.getTransportOptions();
    executorFactory = transportOptions.getExecutorFactory();
    executor = executorFactory.get();
    try {
      StorageSettingsBuilder settingsBuilder =
          new StorageSettingsBuilder(StorageSettings.newBuilder().build());

      settingsBuilder.setCredentialsProvider(options.getCredentialsProvider());
      settingsBuilder.setTransportChannelProvider(options.getTransportChannelProvider());

      HeaderProvider internalHeaderProvider =
          StorageSettings.defaultApiClientHeaderProviderBuilder()
              .setClientLibToken(
                  ServiceOptions.getGoogApiClientLibName(),
                  GaxProperties.getLibraryVersion(options.getClass()))
              .build();

      settingsBuilder.setInternalHeaderProvider(internalHeaderProvider);
      settingsBuilder.setHeaderProvider(options.getMergedHeaderProvider(new NoHeaderProvider()));

      clientContext = ClientContext.create(settingsBuilder.build());

      StorageStubSettings.Builder storageBuilder = StorageStubSettings.newBuilder(clientContext);
      storageStub = GrpcStorageStub.create(storageBuilder.build());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private class DefaultRpcBatch implements RpcBatch {

    // Batch size is limited as, due to some current service implementation details, the service
    // performs better if the batches are split for better distribution. See
    // https://github.com/googleapis/google-cloud-java/pull/952#issuecomment-213466772 for
    // background.
    private static final int MAX_BATCH_SIZE = 100;

    private final Storage storage;
    private final LinkedList<BatchRequest> batches;
    private int currentBatchSize;

    private DefaultRpcBatch(Storage storage) {
      throw new NotImplementedException("Not implemented.");
    }

    @Override
    public void addDelete(
        StorageObject storageObject, Callback<Void> callback, Map<Option, ?> options) {
      throw new NotImplementedException("Not implemented.");
    }

    @Override
    public void addPatch(
        StorageObject storageObject, Callback<StorageObject> callback, Map<Option, ?> options) {
      throw new NotImplementedException("Not implemented.");
    }

    @Override
    public void addGet(
        StorageObject storageObject, Callback<StorageObject> callback, Map<Option, ?> options) {
      throw new NotImplementedException("Not implemented.");
    }

    @Override
    public void submit() {
      throw new NotImplementedException("Not implemented.");
    }
  }

  private static <T> JsonBatchCallback<T> toJsonCallback(final RpcBatch.Callback<T> callback) {
    throw new NotImplementedException("Not implemented.");
  }

  private static StorageException translate(IOException exception) {
    return new StorageException(exception);
  }

  private static StorageException translate(GoogleJsonError exception) {
    return new StorageException(exception);
  }

  private static void setEncryptionHeaders(
      HttpHeaders headers, String headerPrefix, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  /** Helper method to start a span. */
  private Span startSpan(String spanName) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public Bucket create(Bucket bucket, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public StorageObject create(
      StorageObject storageObject, final InputStream content, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public Tuple<String, Iterable<Bucket>> list(Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public Tuple<String, Iterable<StorageObject>> list(final String bucket, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public Bucket get(Bucket bucket, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  private Get getCall(StorageObject object, Map<Option, ?> options) throws IOException {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public StorageObject get(StorageObject object, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public Bucket patch(Bucket bucket, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  private Storage.Objects.Patch patchCall(StorageObject storageObject, Map<Option, ?> options)
      throws IOException {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public StorageObject patch(StorageObject storageObject, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public boolean delete(Bucket bucket, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  private Storage.Objects.Delete deleteCall(StorageObject blob, Map<Option, ?> options)
      throws IOException {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public boolean delete(StorageObject blob, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public StorageObject compose(
      Iterable<StorageObject> sources, StorageObject target, Map<Option, ?> targetOptions) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public byte[] load(StorageObject from, Map<Option, ?> options) {
    ServerStreamingCallable<GetObjectMediaRequest, GetObjectMediaResponse> objectMediaCallable =
        storageStub.getObjectMediaCallable();
    GetObjectMediaRequest getObjectMediaRequest =
        GetObjectMediaRequest.newBuilder()
            .setBucket(from.getBucket().toString())
            .setObject(from.getName())
            .build();
    ServerStream<GetObjectMediaResponse> responses =
        objectMediaCallable.call(getObjectMediaRequest);
    return responses.iterator().next().getChecksummedData().getContent().toByteArray();
  }

  @Override
  public RpcBatch createBatch() {
    throw new NotImplementedException("Not implemented.");
  }

  private Get createReadRequest(StorageObject from, Map<Option, ?> options) throws IOException {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public long read(
      StorageObject from, Map<Option, ?> options, long position, OutputStream outputStream) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public Tuple<String, byte[]> read(
      StorageObject from, Map<Option, ?> options, long position, int bytes) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public void write(
      String uploadId,
      byte[] toWrite,
      int toWriteOffset,
      long destOffset,
      int length,
      boolean last) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public String open(StorageObject object, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public String open(String signedURL) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public RewriteResponse openRewrite(RewriteRequest rewriteRequest) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public RewriteResponse continueRewrite(RewriteResponse previousResponse) {
    throw new NotImplementedException("Not implemented.");
  }

  private RewriteResponse rewrite(RewriteRequest req, String token) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public BucketAccessControl getAcl(String bucket, String entity, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public boolean deleteAcl(String bucket, String entity, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public BucketAccessControl createAcl(BucketAccessControl acl, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public BucketAccessControl patchAcl(BucketAccessControl acl, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public List<BucketAccessControl> listAcls(String bucket, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public ObjectAccessControl getDefaultAcl(String bucket, String entity) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public boolean deleteDefaultAcl(String bucket, String entity) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public ObjectAccessControl createDefaultAcl(ObjectAccessControl acl) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public ObjectAccessControl patchDefaultAcl(ObjectAccessControl acl) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public List<ObjectAccessControl> listDefaultAcls(String bucket) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public ObjectAccessControl getAcl(String bucket, String object, Long generation, String entity) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public boolean deleteAcl(String bucket, String object, Long generation, String entity) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public ObjectAccessControl createAcl(ObjectAccessControl acl) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public ObjectAccessControl patchAcl(ObjectAccessControl acl) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public List<ObjectAccessControl> listAcls(String bucket, String object, Long generation) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public HmacKey createHmacKey(String serviceAccountEmail, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public Tuple<String, Iterable<HmacKeyMetadata>> listHmacKeys(Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public HmacKeyMetadata getHmacKey(String accessId, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public HmacKeyMetadata updateHmacKey(HmacKeyMetadata hmacKeyMetadata, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public void deleteHmacKey(HmacKeyMetadata hmacKeyMetadata, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public Policy getIamPolicy(String bucket, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public Policy setIamPolicy(String bucket, Policy policy, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public TestIamPermissionsResponse testIamPermissions(
      String bucket, List<String> permissions, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public boolean deleteNotification(String bucket, String notification) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public List<Notification> listNotifications(String bucket) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public Notification createNotification(String bucket, Notification notification) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public Bucket lockRetentionPolicy(Bucket bucket, Map<Option, ?> options) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public ServiceAccount getServiceAccount(String projectId) {
    throw new NotImplementedException("Not implemented.");
  }

  // This class is needed solely to get access to protected method setInternalHeaderProvider()
  private static class StorageSettingsBuilder extends StorageSettings.Builder {
    private StorageSettingsBuilder(StorageSettings settings) {
      super(settings);
    }

    @Override
    protected StorageSettings.Builder setInternalHeaderProvider(
        HeaderProvider internalHeaderProvider) {
      return super.setInternalHeaderProvider(internalHeaderProvider);
    }
  }
}
