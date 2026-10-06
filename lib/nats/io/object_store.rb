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
require "digest"
require "stringio"
require "time"

require_relative "../nuid"
require_relative "object_store/api"
require_relative "object_store/errors"
require_relative "object_store/manager"

module NATS
  # ObjectStore stores large objects on top of JetStream, using the same
  # wire protocol as the Object Store clients of the other NATS languages.
  #
  # An object is split into chunks. The chunks are published first, so that
  # a partially uploaded object is never visible: the object becomes
  # visible only when its metadata message, which binds the number of
  # chunks, the size and the SHA-256 digest, is published.
  class ObjectStore
    include MonitorMixin

    # Subject prefixes and headers of the Object Store protocol.
    HDR_CHUNK_SIZE = "Nats-Object-Store-Chunk-Size"
    HDR_DELETE = "Nats-Object-Store-Deleted"
    ROLLUP = "Nats-Rollup"
    MSG_ROLLUP_SUBJECT = "sub"

    DEFAULT_CHUNK_SIZE = 128 * 1024

    # Maximum number of links followed while resolving an object.
    MAX_LINK_DEPTH = 12

    VALID_NAME_RE = /\A[-\/_=.a-zA-Z0-9]+\z/

    class << self
      # Checks whether an object name is valid on the wire. Names use the
      # same character set as KeyValue keys and cannot start or end with a
      # dot, which would produce empty subject tokens.
      def valid_name?(name)
        return false unless name.is_a?(String) && !name.empty?
        return false if name.start_with?(".") || name.end_with?(".")

        name.match?(VALID_NAME_RE)
      end

      # digest_of computes the protocol digest of a chunk payload: the
      # "SHA-256=" prefix followed by URL-safe base64 of the hash, with
      # padding, as nats.go writes it.
      def digest_of(data)
        "SHA-256=" + Base64.urlsafe_encode64(Digest::SHA256.digest(data), padding: true)
      end
    end

    def initialize(opts = {})
      super()
      @name = opts[:name]
      @stream = opts[:stream]
      @js = opts[:js]
      @pre = "$O.#{@name}."
      @chunks_pre = "#{@pre}C."
      @meta_pre = "#{@pre}M."
    end

    attr_reader :name

    # put uploads an object from a String or an IO-like object. It reads
    # at most :chunk_size bytes at a time, so that memory usage grows with
    # the chunk size rather than with the size of the object.
    #
    # @param name [String] Object name.
    # @param io [String, IO] Object contents. Strings are read as one IO.
    # @param params [Hash] Upload options.
    # @option params [Integer] :chunk_size Maximum bytes per chunk.
    # @option params [String] :description Free-form description.
    # @option params [Hash] :headers User headers stored in the metadata.
    # @option params [Hash] :metadata User metadata stored in the metadata.
    # @return [NATS::ObjectStore::API::ObjectInfo] Metadata of the object.
    def put(name, io = nil, params = {})
      raise InvalidObjectNameError.new unless ObjectStore.valid_name?(name)

      io = StringIO.new("") if io.nil?
      io = StringIO.new(io) if io.is_a?(String)

      chunk_size = params[:chunk_size] || params[:chunks_size] || DEFAULT_CHUNK_SIZE
      unless chunk_size.is_a?(Integer) && chunk_size.positive?
        raise ArgumentError.new("nats: chunk_size must be a positive integer")
      end

      nuid = NATS::NUID.next
      chunk_subject = "#{@chunks_pre}#{nuid}"
      hasher = Digest::SHA256.new
      chunks = 0
      size = 0

      begin
        until (payload = io.read(chunk_size)).nil?
          break if payload.empty?

          @js.publish(chunk_subject, payload, stream: @stream)
          hasher.update(payload)
          size += payload.bytesize
          chunks += 1
        end

        info = API::ObjectInfo.new(
          name: name,
          bucket: @name,
          nuid: nuid,
          chunks: chunks,
          size: size,
          digest: "SHA-256=" + Base64.urlsafe_encode64(hasher.digest, padding: true),
          mod_time: Time.now,
          description: params[:description],
          headers: params[:headers],
          metadata: params[:metadata],
          chunk_size: chunk_size
        )
        publish_meta(nuid, info)
      rescue => err
        # The chunks published so far are not referenced by any metadata
        # message, so remove them instead of leaving garbage behind.
        begin
          delete_chunks(nuid)
        rescue
          # Keep the original failure.
        end
        raise err
      end

      info
    end

    # get downloads an object and verifies its chunk sequence, size and
    # digest. With a block, each verified chunk is yielded as it arrives;
    # without one it returns an Enumerator.
    #
    # @param name [String] Object name.
    # @param params [Hash] Download options.
    # @option params [Integer] :timeout Timeout between chunks.
    # @yield [chunk, info] Each chunk, in order, and the object metadata.
    # @return [Enumerator, API::ObjectInfo] Enumerator without a block, or
    #   the metadata with one.
    def get(name, params = {}, &block)
      info, source = resolve(name, params)
      return to_enum(:get, name, params) unless block

      source.send(:stream_chunks, info, params, &block)
      info
    end

    # get_info returns the metadata of an object, following links.
    def get_info(name, params = {})
      object_info(name, params)
    end

    # delete marks an object as deleted and removes the chunks of its
    # current version. The chunks of a linked object are not touched.
    #
    # @return [void]
    def delete(name, params = {})
      info = raw_object_info(name, params)
      if info.deleted
        raise ObjectDeletedError.new(name)
      end

      deleted = API::ObjectInfo.new(
        name: info.name,
        bucket: @name,
        nuid: info.nuid,
        chunks: info.chunks,
        size: info.size,
        mod_time: Time.now,
        chunk_size: info.chunk_size,
        deleted: true
      )
      @js.publish("#{@meta_pre}#{info.nuid}", deleted.to_json,
        header: { HDR_DELETE => "true" }, stream: @stream)
      delete_chunks(info.nuid) unless info.link
      nil
    end

    # add_link makes object +name+ a link to another object. The target
    # may be in this bucket or in another one; a target given as a bucket
    # and no object name becomes a whole-bucket link.
    #
    # @param name [String] Name of the new link object.
    # @param target_name [String] Name of the linked object.
    # @param params [Hash] Link options.
    # @option params [String] :target_bucket Bucket of the linked object;
    #   defaults to this bucket, which is how cross-bucket links are made.
    # @return [API::ObjectInfo] Metadata of the link object.
    def add_link(name, target_name, params = {})
      raise InvalidObjectNameError.new unless ObjectStore.valid_name?(name)
      raise InvalidObjectNameError.new unless ObjectStore.valid_name?(target_name)

      # Links are stored as written; a missing target is reported when the
      # link is resolved, so links may be created before their targets.
      link = API::ObjectLink.new(name: target_name, bucket: params[:target_bucket])

      nuid = NATS::NUID.next
      info = API::ObjectInfo.new(
        name: name,
        bucket: @name,
        nuid: nuid,
        mod_time: Time.now,
        link: link
      )
      publish_meta(nuid, info)
      info
    end

    # add_bucket_link makes object +name+ a link to a whole bucket.
    #
    # @param name [String] Name of the new link object.
    # @param bucket [String] Name of the linked bucket.
    # @return [API::ObjectInfo] Metadata of the link object.
    def add_bucket_link(name, bucket)
      raise InvalidObjectNameError.new unless ObjectStore.valid_name?(name)
      raise InvalidObjectNameError.new unless bucket.is_a?(String) && !bucket.empty?

      nuid = NATS::NUID.next
      info = API::ObjectInfo.new(
        name: name,
        bucket: @name,
        nuid: nuid,
        mod_time: Time.now,
        link: API::ObjectLink.new(bucket: bucket)
      )
      publish_meta(nuid, info)
      info
    end

    # delete_chunks removes all the chunks of one in-progress upload.
    def delete_chunks(nuid)
      @js.publish("#{@chunks_pre}#{nuid}", "",
        header: { ROLLUP => MSG_ROLLUP_SUBJECT }, stream: @stream)
    end

    private

    def publish_meta(nuid, info)
      headers = {}
      headers[HDR_DELETE] = "true" if info.deleted
      if info.chunk_size && info.chunks.positive?
        headers[HDR_CHUNK_SIZE] = info.chunk_size.to_s
      end
      @js.publish("#{@meta_pre}#{nuid}", info.to_json,
        header: headers.empty? ? nil : headers, stream: @stream)
    end

    # raw_object_info finds the latest metadata message for an object name
    # without following links. The metadata subjects include the NUID of
    # the version, so scanning them and taking the highest stream sequence
    # returns exactly one version: an upload that overlaps an older
    # download cannot mix its chunks into the older object.
    def raw_object_info(name, params = {})
      raise InvalidObjectNameError.new unless ObjectStore.valid_name?(name)

      latest = nil
      latest_seq = 0
      seq = 0
      loop do
        msg =
          begin
            @js.get_msg(@stream, next: true, seq: seq, subject: "#{@meta_pre}>")
          rescue NATS::JetStream::Error::NotFound
            break
          end
        seq = msg.seq
        info = API::ObjectInfo.from_json(msg.data, bucket: @name)
        if info.name == name && msg.seq >= latest_seq
          latest = info
          latest_seq = msg.seq
        end
      end

      raise ObjectNotFoundError.new(name) if latest.nil?
      raise ObjectDeletedError.new(name) if latest.deleted

      latest
    end

    # object_info resolves links, limiting the chain and detecting cycles.
    def object_info(name, params = {})
      info, = resolve(name, params)
      info
    end

    # resolve returns the final object metadata and the bucket that holds
    # its chunks after following links.
    def resolve(name, params = {})
      info = raw_object_info(name, params)
      store = self
      seen = { [@name, name] => true }
      depth = 0
      while info.link
        raise TooManyLinksError.new if depth >= MAX_LINK_DEPTH

        target = info.link
        target_bucket = target.bucket || store.name
        key = [target_bucket, target.name]
        raise LinkCycleError.new if seen[key]

        seen[key] = true
        store = store.send(:store_for, target_bucket)
        depth += 1

        # A link without a name points at a whole bucket and cannot be
        # downloaded as a single object.
        if target.name.nil? || target.name.empty?
          raise InvalidLinkError.new("nats: link points to a bucket, not an object")
        end

        info = store.send(:raw_object_info, target.name, params)
        raise ObjectDeletedError.new(target.name) if info.deleted
      end

      [info, store]
    end

    def store_for(bucket) # rubocop:disable Naming/AccessorMethodName
      return self if bucket.nil? || bucket == @name

      @js.object_store(bucket)
    end

    # stream_chunks fetches the chunks of one version in order and checks
    # the sequence, the total size and the digest as the caller consumes.
    def stream_chunks(info, params = {})
      digest = Digest::SHA256.new
      received = 0
      size = 0
      seq = 0

      info.chunks.times do |index|
        msg =
          begin
            @js.get_msg(@stream, next: true, seq: seq,
              subject: "#{@chunks_pre}#{info.nuid}")
          rescue NATS::JetStream::Error::NotFound
            raise InvalidChainError.new(
              "nats: expected #{info.chunks} chunks for '#{info.name}', got #{received}"
            )
          end
        seq = msg.seq
        payload = msg.data
        if payload.nil? || payload.empty?
          raise InvalidChainError.new("nats: empty chunk #{index} for '#{info.name}'")
        end

        digest.update(payload)
        size += payload.bytesize
        received += 1
        yield payload, info
      end

      if size != info.size
        raise InvalidChainError.new(
          "nats: size mismatch for '#{info.name}': metadata=#{info.size} chunks=#{size}"
        )
      end
      actual = "SHA-256=" + Base64.urlsafe_encode64(digest.digest, padding: true)
      if actual != info.digest
        raise InvalidChainError.new("nats: digest mismatch for '#{info.name}'")
      end
    end
  end
end
