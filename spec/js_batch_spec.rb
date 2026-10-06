# frozen_string_literal: true

describe "JetStream atomic batch publish" do
  # FakeBatchConnection is a protocol-level fake NATS connection: it
  # records the messages published to it, answers commit requests as
  # directed, and can drop the connection on command.
  class FakeBatchConnection
    attr_reader :published, :server_info

    def initialize(version: "2.12.0", fail_publish_at: nil, &on_commit)
      @published = []
      @server_info = version ? {version: version} : {}
      @fail_publish_at = fail_publish_at
      @on_commit = on_commit
    end

    def publish_msg(msg)
      raise NATS::IO::ConnectionClosedError if @fail_publish_at == @published.size

      @published << msg
    end

    def request_msg(msg, **_opts)
      @published << msg
      @on_commit.call(msg)
    end
  end

  BATCH_ID = NATS::JetStream::Header::BATCH_ID
  BATCH_SEQUENCE = NATS::JetStream::Header::BATCH_SEQUENCE
  BATCH_COMMIT = NATS::JetStream::Header::BATCH_COMMIT

  let(:messages) do
    [
      {subject: "events.1", payload: "a"},
      {subject: "events.2", payload: "b"},
      {subject: "events.3", payload: "c"}
    ]
  end

  def ack_msg(**fields)
    NATS::Msg.new(subject: "ack", data: fields.to_json)
  end

  def commit_ack(commit, **fields)
    ack_msg(**{stream: "EVENTS", seq: 42, batch: commit.header[BATCH_ID], count: 3}.merge(fields))
  end

  it "publishes the messages in order with batch headers and returns the commit ack" do
    nc = FakeBatchConnection.new { |commit| commit_ack(commit) }
    js = NATS::JetStream.new(nc)

    ack = js.publish_batch(messages)

    expect(nc.published.map(&:subject)).to eql(%w[events.1 events.2 events.3])
    expect(nc.published.map(&:data)).to eql(%w[a b c])
    batch_ids = nc.published.map { |msg| msg.header[BATCH_ID] }.uniq
    expect(batch_ids.size).to eql(1)
    expect(batch_ids.first.bytesize).to be_between(1, 64)
    expect(nc.published.map { |msg| msg.header[BATCH_SEQUENCE] }).to eql(%w[1 2 3])
    expect(nc.published[0..1].map { |msg| msg.header[BATCH_COMMIT] }).to eql([nil, nil])
    expect(nc.published.last.header[BATCH_COMMIT]).to eql("1")

    expect(ack.stream).to eql("EVENTS")
    expect(ack.seq).to eql(42)
    expect(ack.batch).to eql(batch_ids.first)
    expect(ack.count).to eql(3)
    expect(ack.duplicate).to be_falsey
  end

  it "rejects an empty batch before sending anything" do
    nc = FakeBatchConnection.new
    js = NATS::JetStream.new(nc)

    expect { js.publish_batch([]) }.to raise_error(ArgumentError, /non-empty/)
    expect { js.publish_batch(nil) }.to raise_error(ArgumentError, /non-empty/)
    expect(nc.published).to be_empty
  end

  it "rejects a batch over the maximum size before sending anything" do
    nc = FakeBatchConnection.new
    js = NATS::JetStream.new(nc)
    too_many = (NATS::JetStream::MAX_BATCH_SIZE + 1).times.map { |i| {subject: "events.#{i}"} }

    expect { js.publish_batch(too_many) }.to raise_error(ArgumentError, /#{NATS::JetStream::MAX_BATCH_SIZE}/)
    expect(nc.published).to be_empty
  end

  it "rejects invalid entries and batch ids before sending anything" do
    nc = FakeBatchConnection.new
    js = NATS::JetStream.new(nc)

    expect { js.publish_batch([{payload: "a"}]) }.to raise_error(ArgumentError, /subject/)
    expect { js.publish_batch([{subject: "events.1", payload: 1}]) }.to raise_error(ArgumentError, /payload/)
    expect { js.publish_batch(messages, batch_id: "x" * 65) }.to raise_error(ArgumentError, /batch_id/)
    expect(nc.published).to be_empty
  end

  it "rejects the batch before sending anything when the server is too old" do
    nc = FakeBatchConnection.new(version: "2.11.4")
    js = NATS::JetStream.new(nc)

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BatchNotSupported, /2.12.0/)
    expect(nc.published).to be_empty
  end

  it "raises the server error when the commit is refused" do
    nc = FakeBatchConnection.new do |_commit|
      ack_msg(error: {code: 400, err_code: 10183, description: "atomic batch not allowed"})
    end
    js = NATS::JetStream.new(nc)

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BadRequest, /atomic batch not allowed/)
  end

  it "raises BatchNotCommitted when sending fails before the commit" do
    nc = FakeBatchConnection.new(fail_publish_at: 1)
    js = NATS::JetStream.new(nc)

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BatchNotCommitted) { |error|
      expect(error.batch_id).to be_a(String)
      expect(error.batch_id).not_to be_empty
    }
    # The commit message was never sent, so the batch cannot be committed.
    expect(nc.published.size).to eql(1)
    expect(nc.published.none? { |msg| msg.header[BATCH_COMMIT] }).to be(true)
  end

  it "raises BatchCommitUnknown when the connection drops after the commit was sent" do
    nc = FakeBatchConnection.new { raise NATS::IO::ConnectionClosedError }
    js = NATS::JetStream.new(nc)
    error = nil

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BatchCommitUnknown) { |e| error = e }
    expect(nc.published.size).to eql(3)
    expect(nc.published.last.header[BATCH_COMMIT]).to eql("1")

    # The caller can retry the commit with the same batch id; the server
    # commits a batch id only once and answers a duplicate ack.
    nc2 = FakeBatchConnection.new { |commit| commit_ack(commit, duplicate: true) }
    ack = NATS::JetStream.new(nc2).publish_batch(messages, batch_id: error.batch_id)
    expect(ack.batch).to eql(error.batch_id)
    expect(ack.duplicate).to be(true)
    expect(ack.count).to eql(3)
  end

  it "raises BatchCommitUnknown when the commit ack never arrives" do
    nc = FakeBatchConnection.new { raise NATS::Timeout }
    js = NATS::JetStream.new(nc)

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BatchCommitUnknown) { |error|
      expect(error.batch_id).to be_a(String)
    }
  end

  it "raises BatchAckMismatch when the ack confirms another count" do
    nc = FakeBatchConnection.new { |commit| commit_ack(commit, count: 2) }
    js = NATS::JetStream.new(nc)

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BatchAckMismatch) { |error|
      expect(error.batch_id).to eql(nc.published.last.header[BATCH_ID])
      expect(error.ack.count).to eql(2)
      expect(error.ack.batch).to eql(error.batch_id)
    }
  end

  it "raises BatchAckMismatch when the ack names another batch" do
    nc = FakeBatchConnection.new { |commit| commit_ack(commit, batch: "some-other-batch") }
    js = NATS::JetStream.new(nc)

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BatchAckMismatch)
  end

  it "returns a duplicate ack when the batch was already committed" do
    nc = FakeBatchConnection.new { |commit| commit_ack(commit, duplicate: true) }
    js = NATS::JetStream.new(nc)

    ack = js.publish_batch(messages)
    expect(ack.duplicate).to be(true)
    expect(ack.batch).to eql(nc.published.last.header[BATCH_ID])
    expect(ack.count).to eql(3)
  end
end

