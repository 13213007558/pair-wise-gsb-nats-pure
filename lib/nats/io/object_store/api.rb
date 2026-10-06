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

require "base64"
require "json"
require "time"

module NATS
  class ObjectStore
    # Wire types of the NATS Object Store protocol. They are compatible
    # with the Object Store implementations of the other NATS clients.
    module API
      # ObjectStoreConfig configures the JetStream stream that backs a
      # bucket when it is created.
      ObjectStoreConfig = Struct.new(
        :bucket,
        :description,
        :ttl,
        :max_bytes,
        :storage,
        :replicas,
        :placement,
        :compression,
        # Freeform metadata of the bucket (requires nats-server v2.10.0).
        :metadata,
        keyword_init: true
      ) do
        def initialize(opts = {})
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super
        end
      end

      # ObjectLink links an object to another object. A link with a bucket
      # and no name is a whole-bucket link.
      ObjectLink = Struct.new(:name, :bucket, keyword_init: true) do
        def initialize(opts = {})
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super
        end
      end

      # ObjectInfo is the metadata of a stored object. The wire format is
      # JSON with the exact lowercase field names of the Object Store
      # protocol, so buckets are interoperable with the other clients.
      class ObjectInfo
        attr_accessor :name, :description, :bucket
        attr_accessor :nuid, :size, :chunks, :deleted
        attr_accessor :digest, :mod_time, :headers, :link, :metadata
        attr_accessor :chunk_size

        def initialize(opts = {})
          @name = opts[:name]
          @description = opts[:description]
          @bucket = opts[:bucket]
          @nuid = opts[:nuid]
          @size = opts[:size] || 0
          @chunks = opts[:chunks] || 0
          @digest = opts[:digest]
          @deleted = opts[:deleted] || false
          @mod_time = opts[:mod_time]
          @headers = opts[:headers]
          @metadata = opts[:metadata]
          @chunk_size = opts[:chunk_size]
          @link = opts[:link]
        end

        def to_json(*)
          wire = {
            "name" => @name,
            "bucket" => @bucket,
            "nuid" => @nuid,
            "chunks" => @chunks,
            "size" => @size,
            "digest" => @digest,
            "deleted" => @deleted,
            "mtime" => (@mod_time.getutc.iso8601(9) if @mod_time)
          }
          options = {}
          if @link
            options["link"] = { "name" => @link.name, "bucket" => @link.bucket }.compact
          end
          options["max_chunk_size"] = @chunk_size if @chunk_size
          options["headers"] = self.class.encode_headers(@headers) if @headers && !@headers.empty?
          options["desc"] = @description if @description
          options["user_metadata"] = @metadata if @metadata && !@metadata.empty?
          wire["options"] = options unless options.empty?
          wire.compact.to_json
        end

        def self.from_json(data, bucket: nil)
          hash = JSON.parse(data)
          options = hash["options"] || {}
          link = nil
          if (raw_link = options["link"])
            link = ObjectLink.new(
              name: raw_link["name"],
              bucket: raw_link["bucket"]
            )
          end
          new(
            name: hash["name"],
            description: options["desc"],
            bucket: hash["bucket"] || bucket,
            nuid: hash["nuid"],
            chunks: hash["chunks"],
            size: hash["size"],
            digest: hash["digest"],
            deleted: hash["deleted"] || false,
            mod_time: (Time.parse(hash["mtime"]) if hash["mtime"]),
            headers: decode_headers(options["headers"]),
            link: link,
            metadata: options["user_metadata"],
            chunk_size: options["max_chunk_size"]
          )
        end

        # encode_headers serializes user headers as the protocol stores
        # them: MIME-like header text, base64-encoded inside options.
        def self.encode_headers(headers)
          text = +"NATS/1.0\r\n"
          headers.each do |key, value|
            text << "#{key}: #{value}\r\n"
          end
          Base64.strict_encode64(text)
        end

        def self.decode_headers(encoded)
          return unless encoded

          decoded = Base64.strict_decode64(encoded)
          headers = {}
          decoded.each_line do |line|
            line = line.strip
            next if line.empty? || line.start_with?("NATS/")

            key, value = line.split(/:\s*/, 2)
            headers[key] = value
          end
          headers
        end
      end
    end
  end
end
