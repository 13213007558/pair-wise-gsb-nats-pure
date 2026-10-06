# frozen_string_literal: true

require "json"
require "time"

module NATS
  class ObjectStore
    module API
      DEFAULT_CHUNK_SIZE = 128 * 1024
      MAX_CHUNK_SIZE = 1024 * 1024
      DEFAULT_REPLICAS = 1

      ObjectStoreConfig = Struct.new(
        :bucket,
        :description,
        :ttl,
        :max_bytes,
        :storage,
        :replicas,
        :placement,
        keyword_init: true
      ) do
        def initialize(opts = {})
          opts = opts.dup
          rem = opts.keys - members
          opts.delete_if { |key| rem.include?(key) }
          super
        end
      end

      ObjectLink = Struct.new(:bucket, :name, keyword_init: true) do
        def self.from_json(opts = {})
          new(bucket: opts[:bucket], name: opts[:object])
        end

        def initialize(opts = {})
          opts = opts.dup
          rem = opts.keys - members
          opts.delete_if { |key| rem.include?(key) }
          super
        end

        def to_json(*args)
          to_h.compact.tap { |hash| hash[:object] = hash.delete(:name) if hash.key?(:name) }.to_json(*args)
        end

        def link?
          !bucket.nil? && !bucket.empty?
        end

        def bucket_link?
          link? && name.nil?
        end
      end

      ObjectInfo = Struct.new(
        :name,
        :description,
        :size,
        :chunks,
        :digest,
        :modified,
        :nuid,
        :deleted,
        :link,
        keyword_init: true
      ) do
        def self.from_json(data)
          opts = case data
          when String
            return new(name: nil) if data.empty?
            begin
              JSON.parse(data, symbolize_names: true)
            rescue JSON::ParserError
              raise NATS::ObjectStore::InvalidObjectError.new("nats: invalid object metadata")
            end
          when Hash
            data.dup
          else
            raise NATS::ObjectStore::InvalidObjectError.new("nats: invalid object metadata")
          end

          opts[:modified] = Time.parse(opts[:mtime]) if opts[:mtime]
          opts[:link] = ObjectLink.from_json(opts[:options]&.dig(:link) || {}) if opts.dig(:options, :link)
          opts.delete(:mtime)
          opts.delete(:options)
          rem = opts.keys - members
          opts.delete_if { |key| rem.include?(key) }
          info = new(opts)
          raise NATS::ObjectStore::InvalidObjectError.new("nats: object metadata is missing a name") unless info.name

          info
        end

        def to_json(*_args)
          hash = {
            name: name,
            description: description,
            size: size || 0,
            chunks: chunks || 0,
            deleted: true
          }
          hash[:digest] = digest if digest
          hash[:mtime] = modified.utc.iso8601(9) if modified
          hash[:nuid] = nuid if nuid
          hash[:options] = {link: JSON.parse(link.to_json)} if link
          hash.delete(:deleted) unless deleted
          hash.compact.to_json
        end

        def link?
          !link.nil? && link.link?
        end

        def bucket_link?
          link&.bucket_link? || false
        end
      end
    end
  end
end
