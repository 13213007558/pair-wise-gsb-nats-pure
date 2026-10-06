# frozen_string_literal: true

# Copyright 2026 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module NATS
  class ObjectStore
    # Manager adds the object_store, create_object_store and
    # delete_object_store entry points to a JetStream context.
    module Manager
      VALID_BUCKET_RE = /\A[a-zA-Z0-9_-]+\z/

      # object_store binds to an existing Object Store bucket.
      def object_store(bucket)
        stream = "OBJ_#{bucket}"
        begin
          si = stream_info(stream)
        rescue NATS::JetStream::Error::NotFound
          raise ObjectStore::BucketNotFoundError.new
        end

        subjects = si.config.subjects || []
        unless subjects.include?("$O.#{bucket}.>")
          raise ObjectStore::BadBucketError.new("nats: bad bucket '#{bucket}'")
        end

        ObjectStore.new(name: bucket, stream: stream, js: self)
      end

      # create_object_store creates the JetStream stream of a bucket and
      # returns a handle to it.
      def create_object_store(config)
        config =
          if config.is_a?(ObjectStore::API::ObjectStoreConfig)
            config
          else
            ObjectStore::API::ObjectStoreConfig.new(config.is_a?(String) ? { bucket: config } : config)
          end
        bucket = config.bucket
        if bucket.nil? || bucket.empty? || bucket !~ VALID_BUCKET_RE
          raise ArgumentError.new("nats: invalid bucket name")
        end

        max_age = config.ttl
        if max_age
          max_age = max_age * ::NATS::NANOSECONDS if max_age.positive?
        end

        stream = JetStream::API::StreamConfig.new(
          name: "OBJ_#{bucket}",
          description: config.description,
          subjects: ["$O.#{bucket}.>"],
          # Object Store keeps all versions and their chunks.
          max_bytes: config.max_bytes || -1,
          max_consumers: -1,
          max_msgs: -1,
          max_age: max_age,
          storage: config.storage,
          num_replicas: config.replicas || 1,
          placement: config.placement,
          # Rollup headers clean up the temporary chunks of failed uploads
          # and the chunks of deleted objects.
          allow_rollup_hdrs: true,
          discard: "old",
          metadata: config.metadata,
          compression: config.compression ? "s2" : nil
        )

        add_stream(stream)
        ObjectStore.new(name: bucket, stream: stream.name, js: self)
      end

      # delete_object_store deletes the stream that backs a bucket.
      def delete_object_store(bucket)
        delete_stream("OBJ_#{bucket}")
      end
    end
  end
end
