# frozen_string_literal: true

require "base64"
require "digest"
require "set"
require "stringio"

require_relative "object_store/api"
require_relative "object_store/errors"

module NATS
  class ObjectStore
    VALID_OBJECT_RE = /\A[-\w=\/ .]+\z/
    MAX_LINK_DEPTH = 32

    attr_reader :name, :js, :stream

    def initialize(opts = {})
      @name = opts[:name]
      @stream = opts[:stream]
      @js = opts[:js]
      @direct = opts[:direct]
    end

    def put(object_name, source = nil, params = {})
      validate_object_name(object_name)
      chunk_size = params[:chunk_size] || API::DEFAULT_CHUNK_SIZE
      unless chunk_size.is_a?(Integer) && chunk_size.positive? && chunk_size <= API::MAX_CHUNK_SIZE
        raise InvalidObjectOptionsError
      end

      source = StringIO.new(source.to_s) if source.nil? || source.is_a?(String)
      raise InvalidObjectOptionsError.new("nats: source must respond to read") unless source.respond_to?(:read)

      id = NATS::NUID.next
      chunks = 0
      size = 0
      checksum = Digest::SHA256.new
      chunks_subject = chunk_subject(id)
      old_info = begin
        object_info(object_name)
      rescue ObjectNotFoundError
        nil
      end

      begin
        buffer = +""
        while (chunk = source.read(chunk_size, buffer))
          raise SizeMismatchError if chunk.bytesize > chunk_size

          @js.publish(chunks_subject, chunk, header: {
            NATS::JetStream::Header::MSG_ID => "#{id}-#{chunks}"
          })
          chunks += 1
          size += chunk.bytesize
          checksum.update(chunk)
          buffer = +""
        end

        info = API::ObjectInfo.new(
          name: object_name,
          description: params[:description],
          size: size,
          chunks: chunks,
          digest: encode_digest(checksum),
          modified: Time.now,
          nuid: id
        )
        @js.publish(object_subject(object_name), info.to_json)
      rescue => error
        begin
          @js.purge_stream(@stream, filter: chunks_subject)
        rescue
          raise Error.new("nats: failed to clean incomplete object upload")
        end
        raise error
      end

      purge_old_chunks(old_info, id)
      info
    end

    def get(object_name, &block)
      target_store, info = resolve_target(object_name)
      raise ObjectDeletedError if info.deleted
      raise InvalidLinkError if info.bucket_link?
      complete_metadata = info.nuid && info.chunks && info.size && info.digest
      raise InvalidObjectError.new("nats: object metadata is incomplete") unless complete_metadata

      chunks = target_store.chunk_enumerator(info)
      if block
        chunks.each(&block)
        info
      else
        chunks
      end
    end
    alias_method :each_chunk, :get

    def object_info(object_name, params = {})
      info = raw_object_info(object_name)
      params[:follow] ? follow_link(info, Set.new, object_name) : info
    end
    alias_method :info, :object_info

    def delete(object_name)
      old_info = raw_object_info(object_name)
      return true if old_info.deleted

      info = API::ObjectInfo.new(
        name: object_name,
        size: 0,
        chunks: 0,
        modified: Time.now,
        deleted: true
      )
      @js.publish(object_subject(object_name), info.to_json)
      purge_old_chunks(old_info, nil)
      true
    end

    def add_link(object_name, target_store, target_name = nil)
      validate_object_name(object_name)
      target_bucket = target_store.is_a?(ObjectStore) ? target_store.name : target_store
      raise InvalidLinkError unless target_bucket&.is_a?(String) && !target_bucket.empty?
      if !target_name.nil? && (!target_name.is_a?(String) || target_name.empty?)
        raise InvalidLinkError
      end

      old_info = begin
        object_info(object_name)
      rescue ObjectNotFoundError
        nil
      end
      info = API::ObjectInfo.new(
        name: object_name,
        size: 0,
        chunks: 0,
        modified: Time.now,
        link: API::ObjectLink.new(bucket: target_bucket, name: target_name)
      )
      @js.publish(object_subject(object_name), info.to_json)
      purge_old_chunks(old_info, nil)
      info
    end
    alias_method :link, :add_link

    def add_bucket_link(object_name, target_store)
      add_link(object_name, target_store, nil)
    end

    def raw_object_info(object_name)
      validate_object_name(object_name)
      msg = @js.get_msg(@stream, subject: object_subject(object_name), direct: @direct)
      API::ObjectInfo.from_json(msg.data)
    rescue NATS::JetStream::Error::NotFound
      raise ObjectNotFoundError
    rescue JSON::ParserError
      raise InvalidObjectError.new("nats: invalid object metadata")
    end

    def chunk_enumerator(info)
      Enumerator.new do |yield_to|
        checksum = Digest::SHA256.new
        received_size = 0
        sequence = 0
        subject = chunk_subject(info.nuid)

        info.chunks.times do |index|
          params = if index.zero?
            {subject: subject, seq: 0, next: true}
          else
            {subject: subject, seq: sequence, next: true}
          end
          message = @js.get_msg(@stream, **params, direct: @direct)
          raise ChunkMismatchError if message.subject != subject

          next_sequence = message.seq.to_i
          raise ChunkMismatchError if next_sequence <= 0
          raise ChunkMismatchError if !sequence.zero? && next_sequence <= sequence
          sequence = next_sequence
          raise ChunkMismatchError if message.data.bytesize > API::MAX_CHUNK_SIZE
          received_size += message.data.bytesize
          checksum.update(message.data)
          yield_to << message.data
        end

        raise SizeMismatchError unless received_size == info.size
        raise DigestMismatchError unless checksum.digest == decode_digest(info.digest)
      end
    end

    private

    def validate_object_name(object_name)
      return if object_name.is_a?(String) && !object_name.empty? && object_name.match?(VALID_OBJECT_RE) &&
        !object_name.start_with?(".", "/") && !object_name.end_with?(".", "/") &&
        !object_name.include?("..")

      raise InvalidObjectNameError
    end

    def object_subject(object_name)
      "$O.#{@name}.O.#{object_name}"
    end

    def chunk_subject(id)
      "$O.#{@name}.C.#{id}"
    end

    def follow_link(info, visited, object_name = info.name)
      follow_target(info, visited, object_name).last
    end

    def resolve_target(object_name)
      info = raw_object_info(object_name)
      follow_target(info, Set.new, object_name)
    end

    def follow_target(info, visited, object_name = info.name)
      return [self, info] unless info.link?

      depth = 0
      current_store = self
      original_key = "#{current_store.name}/#{object_name}"
      while info.link?
        link = info.link
        raise InvalidLinkError if link.name.nil?
        target = "#{link.bucket}/#{link.name}"
        raise LinkCycleError if visited.include?(target)
        visited << target
        depth += 1
        raise LinkDepthExceededError if depth > MAX_LINK_DEPTH

        current_store = if link.bucket == current_store.name
          current_store
        else
          current_store.js.object_store(link.bucket)
        end
        info = current_store.raw_object_info(link.name)
        resolved_key = "#{current_store.name}/#{info.name}"
        raise LinkCycleError if resolved_key == original_key
        visited << resolved_key
      end
      [current_store, info]
    end

    def encode_digest(checksum)
      "SHA-256=#{Base64.urlsafe_encode64(checksum.digest, padding: false)}"
    end

    def decode_digest(value)
      algorithm, encoded = value.to_s.split("=", 2)
      raise InvalidObjectError unless algorithm == "SHA-256" && encoded && !encoded.empty?

      digest = Base64.urlsafe_decode64(encoded)
      raise InvalidObjectError unless digest.bytesize == 32
      digest
    rescue ArgumentError
      raise InvalidObjectError
    end

    def purge_old_chunks(info, current_id)
      return unless info && !info.deleted && !info.link? && info.nuid && info.nuid != current_id

      @js.purge_stream(@stream, filter: chunk_subject(info.nuid))
    rescue NATS::JetStream::Error
      nil
    end
  end
end
