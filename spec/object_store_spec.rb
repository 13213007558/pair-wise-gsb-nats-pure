# frozen_string_literal: true

require "base64"
require "stringio"

class FakeObjectJetStream
  include NATS::ObjectStore::Manager

  Message = Struct.new(:subject, :data, :header, :seq, keyword_init: true)

  attr_reader :streams, :published, :purges, :deleted_streams, :stream_create_requests

  def initialize
    @streams = {}
    @published = []
    @purges = []
    @deleted_streams = []
    @stream_create_requests = []
    @seq = 0
    @fail_subject = nil
  end

  def fail_publish!(subject)
    @fail_subject = subject
  end

  def messages(stream_name, subject)
    @streams.fetch(stream_name).fetch(:messages).fetch(subject, [])
  end

  def add_stream(config, _params = {})
    config = NATS::JetStream::API::StreamConfig.new(config) unless config.is_a?(NATS::JetStream::API::StreamConfig)
    @stream_create_requests << config
    @streams[config.name] = {config: config, messages: Hash.new { |hash, key| hash[key] = [] }}
    stream_info(config.name)
  end

  def stream_info(stream_name, _params = {})
    raise NATS::JetStream::Error::NotFound unless @streams.key?(stream_name)

    config = @streams.fetch(stream_name).fetch(:config)
    state = NATS::JetStream::API::StreamState.new(messages: 0, bytes: 0, first_seq: 0, last_seq: 0)
    NATS::JetStream::API::StreamInfo.new(
      type: "io.nats.jetstream.api.v1.stream_info_response",
      config: config.to_h,
      created: Time.now.utc.iso8601(9),
      state: state.to_h
    )
  end

  def delete_stream(stream_name, _params = {})
    raise NATS::JetStream::Error::NotFound unless @streams.delete(stream_name)
    @deleted_streams << stream_name
    true
  end

  def publish(subject, payload = "", params = {})
    if @fail_subject && subject.start_with?(@fail_subject)
      raise NATS::JetStream::Error::NoStreamResponse.new("nats: no response from stream")
    end
    raise NATS::IO::NoRespondersError unless stream_for(subject)

    message = Message.new(
      subject: subject,
      data: payload.to_s.dup.force_encoding(Encoding::BINARY),
      header: params[:header] || {},
      seq: next_seq
    )
    @published << message
    stream_for(subject).fetch(:messages)[subject] << message
    NATS::JetStream::PubAck.new(stream: stream_name_for(subject), seq: message.seq, duplicate: false)
  end

  def get_msg(stream_name, params = {})
    raise NATS::JetStream::Error::NotFound unless @streams.key?(stream_name)

    subject = params[:subject]
    stored = stream_messages(stream_name, subject)
    if params[:next]
      message = stored.find { |candidate| candidate.seq > params[:seq].to_i }
    elsif params[:seq]
      all_messages = @streams.fetch(stream_name).fetch(:messages).values.flatten
      message = all_messages.find { |candidate| candidate.seq == params[:seq] }
    else
      message = stored.last
    end
    raise NATS::JetStream::Error::NotFound unless message

    NATS::JetStream::API::RawStreamMsg.new(
      subject: message.subject,
      seq: message.seq,
      data: Base64.strict_encode64(message.data),
      headers: message.header
    )
  end

  def purge_stream(stream_name, params = {})
    filter = params[:filter] || params[:subject] || ">"
    @purges << [stream_name, filter]
    raise NATS::JetStream::Error::NotFound unless @streams.key?(stream_name)

    stream = @streams.fetch(stream_name)
    stream.fetch(:messages).delete_if do |subject, _messages|
      filter == ">" || subject == filter || subject.start_with?("#{filter}.")
    end
    true
  end

  private

  def next_seq
    @seq += 1
  end

  def stream_name_for(subject)
    @streams.keys.find { |name| subject.start_with?(subject_prefix_for(name)) }
  end

  def stream_for(subject)
    name = stream_name_for(subject)
    name && @streams.fetch(name)
  end

  def subject_prefix_for(stream_name)
    "$O.#{stream_name.delete_prefix("OBJ_")}."
  end

  def stream_messages(stream_name, subject)
    @streams.fetch(stream_name).fetch(:messages).fetch(subject, [])
  end
end

class TrackingReader
  attr_reader :reads

  def initialize(data, chunk_size)
    @io = StringIO.new(data)
    @chunk_size = chunk_size
    @reads = []
  end

  def read(length, buffer = nil)
    chunk = @io.read(length, buffer)
    @reads << chunk.bytesize if chunk
    chunk
  end

  def pos
    @io.pos
  end
end

describe "ObjectStore" do
  let(:js) { FakeObjectJetStream.new }
  let(:store) { js.create_object_store(bucket: "FILES", storage: "file", replicas: 3) }

  def metadata_message(name = "report.txt")
    js.published.select { |message| message.subject == "$O.FILES.O.#{name}" }.last
  end

  def metadata(name = "report.txt")
    subjects = js.published.select { |message| message.subject == "$O.FILES.O.#{name}" }
    NATS::ObjectStore::API::ObjectInfo.from_json(subjects.last.data)
  end

  it "creates the protocol stream with Object Store subjects and limits" do
    store
    config = js.stream_create_requests.first

    expect(config.name).to eql("OBJ_FILES")
    expect(config.subjects).to eql(["$O.FILES.>"])
    expect(config.retention).to eql("limits")
    expect(config.discard).to eql("old")
    expect(config.allow_direct).to eql(true)
    expect(config.allow_rollup_hdrs).to eql(true)
    expect(config.num_replicas).to eql(3)
    expect(config.max_age).to be_nil
  end

  it "opens an existing bucket and rejects invalid buckets" do
    store
    expect(js.object_store("FILES").name).to eql("FILES")

    js.add_stream(name: "OBJ_OTHER", subjects: ["unrelated.>"])
    expect { js.object_store("OTHER") }.to raise_error(NATS::ObjectStore::BadBucketError)
    expect { js.object_store("MISSING") }.to raise_error(NATS::ObjectStore::BucketNotFoundError)
  end

  it "uploads chunks and commits final metadata with count, size and digest" do
    reader = TrackingReader.new("ab" * 300, 400)
    info = store.put("report.txt", reader, chunk_size: 400)

    chunks = js.published.select { |message| message.subject.start_with?("$O.FILES.C.") }
    expect(chunks.map { |message| message.data.bytesize }).to eql([400, 200])
    expect(reader.reads).to eql([400, 200])
    expect(chunks[0].header).to eql({NATS::JetStream::Header::MSG_ID => "#{info.nuid}-0"})
    expect(chunks[1].header).to eql({NATS::JetStream::Header::MSG_ID => "#{info.nuid}-1"})
    expect(chunks.map(&:subject).uniq).to eql(["$O.FILES.C.#{info.nuid}"])

    committed = metadata
    expect(committed.name).to eql("report.txt")
    expect(committed.nuid).to eql(info.nuid)
    expect(committed.size).to eql(600)
    expect(committed.chunks).to eql(2)
    digest = Digest::SHA256.digest("ab" * 300)
    expected_digest = "SHA-256=#{Base64.urlsafe_encode64(digest, padding: false)}"
    expect(committed.digest).to eql(expected_digest)
  end

  it "downloads chunks on demand and checks sequence, size and digest" do
    data = "chunk-binary-data" * 40
    store.put("report.txt", StringIO.new(data), chunk_size: 128)
    chunks = []
    returned = store.get("report.txt") { |chunk| chunks << chunk }

    expect(returned.name).to eql("report.txt")
    expect(chunks.length).to eql(6)
    expect(chunks.map(&:bytesize)).to eql([128, 128, 128, 128, 128, 40])
    expect(chunks.join).to eql(data)
    expect(Digest::SHA256.hexdigest(chunks.join)).to eql(Digest::SHA256.hexdigest(data))
  end

  it "keeps an in-flight download bound to the old version during an overwrite" do
    store.put("report.txt", "first-version", chunk_size: 16)
    old_info = metadata
    old_download = store.get("report.txt")

    def store.purge_old_chunks(*)
    end
    store.put("report.txt", "second-version", chunk_size: 16)
    new_info = metadata

    expect(old_download.to_a.join).to eql("first-version")
    expect(new_info.nuid).not_to eql(old_info.nuid)
    expect(js.published.map(&:subject)).to include("$O.FILES.C.#{old_info.nuid}")
    expect(js.published.map(&:subject)).to include("$O.FILES.C.#{new_info.nuid}")
    expect(store.get("report.txt").to_a.join).to eql("second-version")
  end

  it "purges only the temporary chunks from a failed final publish" do
    store
    js.fail_publish!("$O.FILES.O.report.txt")

    expect { store.put("report.txt", "abc", chunk_size: 16) }.to raise_error(NATS::JetStream::Error::NoStreamResponse)
    failed_id = js.published.first.subject.split("C.", 2).last
    expect(js.purges).to eql([["OBJ_FILES", "$O.FILES.C.#{failed_id}"]])
    expect(js.messages("OBJ_FILES", "$O.FILES.C.#{failed_id}")).to be_empty
    expect { store.object_info("report.txt") }.to raise_error(NATS::ObjectStore::ObjectNotFoundError)
  end

  it "reads object information and deletes objects" do
    info = store.put("report.txt", "abc", chunk_size: 16)
    fetched = store.object_info("report.txt")

    expect(fetched.nuid).to eql(info.nuid)
    expect(fetched.size).to eql(3)
    expect(fetched.deleted).to be_nil

    expect(store.delete("report.txt")).to eql(true)
    deleted = store.object_info("report.txt")
    expect(deleted.deleted).to eql(true)
    expect(metadata_message.data).to include("\"deleted\":true")
    expect { store.get("report.txt").to_a }.to raise_error(NATS::ObjectStore::ObjectDeletedError)
    expect(js.purges.last).to eql(["OBJ_FILES", "$O.FILES.C.#{info.nuid}"])
  end

  it "supports links across buckets and resolves them with cycle and depth errors" do
    target = js.create_object_store("ARCHIVE")
    target.put("original.txt", "linked data", chunk_size: 16)
    info = store.add_link("link.txt", target, "original.txt")

    expect(info.link.bucket).to eql("ARCHIVE")
    encoded_link = "\"options\":{\"link\":{\"bucket\":\"ARCHIVE\",\"object\":\"original.txt\"}}"
    expect(metadata_message("link.txt").data).to include(encoded_link)
    expect(metadata("link.txt").link.name).to eql("original.txt")
    expect(store.object_info("link.txt", follow: true).size).to eql(11)
    expect(store.get("link.txt").to_a.join).to eql("linked data")

    store.add_bucket_link("bucket-link.txt", target)
    expect { store.object_info("bucket-link.txt", follow: true) }.to raise_error(NATS::ObjectStore::InvalidLinkError)

    store.add_link("a.txt", store, "b.txt")
    store.add_link("b.txt", store, "a.txt")
    expect { store.object_info("a.txt", follow: true) }.to raise_error(NATS::ObjectStore::LinkCycleError)

    target.add_link("one.txt", store, "b.txt")
    expect { target.object_info("one.txt", follow: true) }.to raise_error(NATS::ObjectStore::LinkCycleError)
  end

  it "rejects corrupted chunk sequences, size and digest" do
    store.put("report.txt", "abc", chunk_size: 16)
    info = metadata
    msg = js.messages("OBJ_FILES", "$O.FILES.C.#{info.nuid}").first
    msg.data = "xyz"

    expect { store.get("report.txt").to_a }.to raise_error(NATS::ObjectStore::DigestMismatchError)

    msg.data = "ab"
    expect { store.get("report.txt").to_a }.to raise_error(NATS::ObjectStore::SizeMismatchError)

    metadata_message.data = JSON.generate(name: "report.txt", size: 3, chunks: 2, nuid: info.nuid,
      digest: info.digest)
    expect { store.get("report.txt").to_a }.to raise_error(NATS::JetStream::Error::NotFound)
  end

  it "supports zero-byte objects" do
    store.put("empty.txt", "", chunk_size: 16)
    info = store.object_info("empty.txt")

    expect(info.size).to eql(0)
    expect(info.chunks).to eql(0)
    expect(store.get("empty.txt").to_a).to eql([])
  end
end
