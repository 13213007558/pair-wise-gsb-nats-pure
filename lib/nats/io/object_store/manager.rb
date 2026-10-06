# frozen_string_literal: true

module NATS
  class ObjectStore
    module Manager
      def object_store(bucket)
        stream = "OBJ_#{bucket}"
        begin
          info = stream_info(stream)
        rescue NATS::JetStream::Error::NotFound
          raise NATS::ObjectStore::BucketNotFoundError
        end

        expected_subjects = ["$O.#{bucket}.>"]
        unless info.config.subjects == expected_subjects
          raise NATS::ObjectStore::BadBucketError
        end

        ObjectStore.new(
          name: bucket,
          stream: stream,
          js: self,
          direct: info.config.allow_direct
        )
      end

      def create_object_store(config)
        config = case config
        when String, NATS::ObjectStore::API::ObjectStoreConfig
          config.is_a?(String) ? NATS::ObjectStore::API::ObjectStoreConfig.new(bucket: config) : config
        when Hash
          NATS::ObjectStore::API::ObjectStoreConfig.new(config)
        else
          raise ArgumentError.new("nats: invalid object store config")
        end
        bucket = config.bucket
        raise NATS::ObjectStore::InvalidObjectStoreNameError unless bucket&.match?(NATS::KeyValue::VALID_BUCKET_RE)

        replicas = config.replicas || NATS::ObjectStore::API::DEFAULT_REPLICAS
        unless replicas.is_a?(Integer) && replicas.positive?
          raise ArgumentError.new("nats: replicas must be a positive integer")
        end

        max_age = if config.ttl
          valid_ttl = config.ttl.is_a?(Integer) && !config.ttl.negative?
          raise ArgumentError.new("nats: ttl must be a non-negative integer") unless valid_ttl
          config.ttl * NATS::NANOSECONDS
        end

        stream = NATS::JetStream::API::StreamConfig.new(
          name: "OBJ_#{bucket}",
          description: config.description,
          subjects: ["$O.#{bucket}.>"],
          retention: "limits",
          max_consumers: -1,
          max_msgs: -1,
          max_bytes: config.max_bytes || -1,
          discard: "old",
          max_age: max_age,
          max_msg_size: -1,
          storage: config.storage || "file",
          num_replicas: replicas,
          placement: config.placement,
          allow_direct: true,
          allow_rollup_hdrs: true
        )
        add_stream(stream)
        object_store(bucket)
      end

      def delete_object_store(bucket)
        delete_stream("OBJ_#{bucket}")
      rescue NATS::JetStream::Error::NotFound
        raise NATS::ObjectStore::BucketNotFoundError
      end

      def purge_stream(stream, params = {})
        filter = params[:filter] || params[:subject] || ">"
        req_subject = "#{@prefix}.STREAM.PURGE.#{stream}"
        result = api_request(req_subject, {filter: filter}.to_json, params.except(:filter, :subject))
        result[:success]
      end
    end
  end
end
