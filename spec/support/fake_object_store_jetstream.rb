# frozen_string_literal: true

require "base64"

# In-memory implementation of the slice of the JetStream API that
# NATS::ObjectStore uses: stream management, publishes, STREAM.MSG.GET reads
# (including next_by_subj) and subject rollups. It records every published
# message so that specs can assert the real Object Store subjects, headers
# and payloads without running a nats-server.
class FakeObjectStoreJetStreamBackend
  Message = Struct.new(:seq, :subject, :data, :headers, keyword_init: true)

  attr_reader :streams, :published

  def initialize
    @streams = {}
    @seq = 0
    @mutex = Mutex.new
    @published = []
  end

  def reset
    @streams.clear
    @published.clear
    @seq = 0
  end

  def add_stream(config)
    @streams[config[:name]] = { config: config, messages: [] }
    config
  end

  def stream_info(name)
    raise NATS::JetStream::Error::NotFound.new unless @streams[name]

    @streams[name]
  end

  def delete_stream(name)
    raise NATS::JetStream::Error::NotFound.new unless @streams.delete(name)

    true
  end

  # Matches a NATS subject filter against a concrete subject: "*" is one
  # token and ">" one or more trailing tokens.
  def subject_match?(pattern, subject)
    p_tokens = pattern.split(".")
    s_tokens = subject.split(".")
    p_tokens.each_with_index do |token, index|
      return s_tokens.size > index if token == ">"
      return false if index >= s_tokens.size
      return false unless token == "*" || token == s_tokens[index]
    end
    s_tokens.size == p_tokens.size
  end

  def publish(stream_name, subject, data, header = nil)
    stream = stream_info(stream_name)
    unless stream[:config][:subjects].any? { |pattern| subject_match?(pattern, subject) }
      raise NATS::JetStream::Error::NoStreamResponse.new("no stream takes #{subject}")
    end

    if header && header[NATS::ObjectStore::ROLLUP] == NATS::ObjectStore::MSG_ROLLUP_SUBJECT
      stream[:messages].delete_if { |msg| msg.subject == subject }
      # The server applies the rollup and does not store the marker.
      @published << Message.new(seq: 0, subject: subject, data: "", headers: header)
      return NATS::JetStream::PubAck.new(stream: stream_name, seq: 0)
    end

    seq = @mutex.synchronize { @seq += 1 }
    msg = Message.new(seq: seq, subject: subject, data: data.to_s.b, headers: header)
    stream[:messages] << msg
    @published << msg
    NATS::JetStream::PubAck.new(stream: stream_name, seq: seq)
  end

  # Emulates STREAM.MSG.GET with next_by_subj: seq=0 asks for the first
  # message on the subject, and a later seq asks for the one after it.
  def get_msg(stream_name, params)
    stream = stream_info(stream_name)
    after_seq = params[:next] ? params[:seq].to_i : 0
    pattern = params[:subject]
    candidates = stream[:messages].select { |candidate| candidate.seq > after_seq }
    candidates = candidates.select { |candidate| subject_match?(pattern, candidate.subject) } if pattern
    msg = candidates.first
    raise NATS::JetStream::Error::NotFound.new unless msg

    # The API base64-encodes the payload, and RawStreamMsg decodes it.
    NATS::JetStream::API::RawStreamMsg.new(
      subject: msg.subject,
      seq: msg.seq,
      data: Base64.strict_encode64(msg.data),
      headers: msg.headers
    )
  end
end

# A stand-in for a JetStream context. It includes the real
# NATS::ObjectStore::Manager so that cross-bucket links go through the same
# entry points as with a live connection.
class FakeObjectStoreJetStream
  include NATS::ObjectStore::Manager

  def initialize(backend)
    @backend = backend
  end

  def publish(subject, data = "", **params)
    @backend.publish(params[:stream], subject, data, params[:header])
  end

  def get_msg(stream, params = {})
    @backend.get_msg(stream, params)
  end

  def stream_info(stream, _params = {})
    info = @backend.stream_info(stream)
    config = NATS::JetStream::API::StreamConfig.new(info[:config])
    NATS::JetStream::API::StreamInfo.new(
      type: "io.nats.jetstream.api.v1.stream_info_response",
      config: config.to_h,
      created: Time.now.utc.iso8601,
      state: { messages: info[:messages].size }
    )
  end

  def add_stream(config, _params = {})
    unless config.is_a?(NATS::JetStream::API::StreamConfig)
      config = NATS::JetStream::API::StreamConfig.new(config)
    end
    @backend.add_stream(config.to_h)
    config
  end

  def delete_stream(stream, _params = {})
    @backend.delete_stream(stream)
  end
end
