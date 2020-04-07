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

package com.google.cloud.storage;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceDefaults;
import com.google.cloud.ServiceOptions;
import com.google.cloud.ServiceRpc;
import com.google.cloud.TransportOptions;
import com.google.cloud.google.storage.v1.StorageSettings;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.spi.StorageRpcFactory;
import com.google.cloud.storage.spi.v1.GrpcStorageRpc;
import com.google.cloud.storage.spi.v1.HttpStorageRpc;
import com.google.cloud.storage.spi.v1.StorageRpc;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Set;

public class StorageOptions extends ServiceOptions<Storage, StorageOptions> {

  private static final long serialVersionUID = -2907268477247502947L;
  private static final String API_SHORT_NAME = "Storage";
  private static final String GCS_SCOPE = "https://www.googleapis.com/auth/devstorage.full_control";
  private static final Set<String> SCOPES = ImmutableSet.of(GCS_SCOPE);
  private static final String DEFAULT_HTTP_HOST = "https://storage.googleapis.com";
  private static final String DEFAULT_GRPC_HOST = "storage.googleapis.com:443";
  private final boolean grpcEnabled;
  private final TransportChannelProvider channelProvider;
  private final CredentialsProvider credentialsProvider;

  public static class DefaultStorageFactory implements StorageFactory {

    private static final StorageFactory INSTANCE = new DefaultStorageFactory();

    @Override
    public Storage create(StorageOptions options) {
      return new StorageImpl(options);
    }
  }

  public static class DefaultStorageRpcFactory implements StorageRpcFactory {

    private static final StorageRpcFactory INSTANCE = new DefaultStorageRpcFactory();

    @Override
    public ServiceRpc create(StorageOptions options) {
      if (options.getGrpcEnabled()) {
        return new GrpcStorageRpc(options);
      } else {
        return new HttpStorageRpc(options);
      }
    }
  }

  public static class Builder extends ServiceOptions.Builder<Storage, StorageOptions, Builder> {

    private boolean grpcEnabled = false;
    private TransportChannelProvider channelProvider = null;
    private CredentialsProvider credentialsProvider = null;

    private Builder() {}

    private Builder(StorageOptions options) {
      super(options);
      this.grpcEnabled = options.grpcEnabled;
      this.channelProvider = options.channelProvider;
      this.credentialsProvider = options.credentialsProvider;
    }

    @Override
    public Builder setTransportOptions(TransportOptions transportOptions) {
      return super.setTransportOptions(transportOptions);
    }

    /**
     * Sets the {@link TransportChannelProvider} to use with this Firestore client.
     *
     * @param channelProvider A InstantiatingGrpcChannelProvider object that defines the transport
     *     provider for this client.
     */
    public Builder setChannelProvider(TransportChannelProvider channelProvider) {
      if (!(channelProvider instanceof InstantiatingGrpcChannelProvider)) {
        throw new IllegalArgumentException(
            "Only GRPC channels are allowed for " + API_SHORT_NAME + ".");
      }
      this.channelProvider = channelProvider;
      return this;
    }

    /**
     * Sets the {@link CredentialsProvider} to use with this Firestore client.
     *
     * @param credentialsProvider A CredentialsProvider object that defines the credential provider
     *     for this client.
     */
    public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public Builder setGrpcEnabled(boolean enabled) {
      this.grpcEnabled = enabled;
      return this;
    }

    @Override
    public StorageOptions build() {
      if (this.credentials == null && this.credentialsProvider != null) {
        try {
          this.setCredentials(credentialsProvider.getCredentials());
        } catch (IOException e) {
          throw new RuntimeException("Failed to obtain credentials", e);
        }
      }
      return new StorageOptions(this);
    }
  }

  private StorageOptions(Builder builder) {
    super(StorageFactory.class, StorageRpcFactory.class, builder, new StorageDefaults());

    this.grpcEnabled = builder.grpcEnabled;

    if (this.grpcEnabled) {
      this.channelProvider =
          builder.channelProvider != null
              ? builder.channelProvider
              : GrpcTransportOptions.setUpChannelProvider(
                  StorageSettings.defaultGrpcTransportProviderBuilder(), this);

      this.credentialsProvider =
          builder.credentialsProvider != null
              ? builder.credentialsProvider
              : GrpcTransportOptions.setUpCredentialsProvider(this);
    } else {
      this.channelProvider = null;
      this.credentialsProvider = null;
    }
  }

  private static class StorageDefaults implements ServiceDefaults<Storage, StorageOptions> {

    @Override
    public StorageFactory getDefaultServiceFactory() {
      return DefaultStorageFactory.INSTANCE;
    }

    @Override
    public StorageRpcFactory getDefaultRpcFactory() {
      return DefaultStorageRpcFactory.INSTANCE;
    }

    @Override
    public TransportOptions getDefaultTransportOptions() {
      return getDefaultHttpTransportOptions();
    }
  }

  public static HttpTransportOptions getDefaultHttpTransportOptions() {
    return HttpTransportOptions.newBuilder().build();
  }

  public static GrpcTransportOptions getDefaultGrpcTransportOptions() {
    return getDefaultTransportOptionsBuilder().build();
  }

  // Project ID is only required for creating buckets, so we don't require it for creating the
  // service.
  @Override
  protected boolean projectIdRequired() {
    return false;
  }

  @Override
  protected Set<String> getScopes() {
    return SCOPES;
  }

  protected StorageRpc getStorageRpcV1() {
    return (StorageRpc) getRpc();
  }

  public static GrpcTransportOptions.Builder getDefaultTransportOptionsBuilder() {
    return GrpcTransportOptions.newBuilder();
  }

  public CredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  public TransportChannelProvider getTransportChannelProvider() {
    return channelProvider;
  }

  public boolean getGrpcEnabled() {
    return grpcEnabled;
  }

  /** Returns a default {@code StorageOptions} instance. */
  public static StorageOptions getDefaultInstance() {
    return newBuilder().build();
  }

  public static StorageOptions getDefaultGrpcInstance() {
    return newBuilder()
        .setTransportOptions(getDefaultGrpcTransportOptions())
        .setGrpcEnabled(true)
        .setHost(DEFAULT_GRPC_HOST)
        .build();
  }

  /** Returns a unauthenticated {@code StorageOptions} instance. */
  public static StorageOptions getUnauthenticatedInstance() {
    return newBuilder().setCredentials(NoCredentials.getInstance()).build();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Builder toBuilder() {
    return new Builder(this).setHost(DEFAULT_HTTP_HOST);
  }

  @Override
  public int hashCode() {
    return baseHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof StorageOptions && baseEquals((StorageOptions) obj);
  }

  public static Builder newBuilder() {
    return new Builder().setHost(DEFAULT_HTTP_HOST);
  }
}
