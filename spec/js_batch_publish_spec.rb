# frozen_string_literal: true

describe "JetStream atomic batch publish" do
  # FakeConnection is a protocol-level fake of NATS::Client: it records
  # the messages published to it and answers requests as scripted, so
  # that the batch protocol can be tested without a server.
  class FakeConnection
    attr_reader :published, :requests
    attr_accessor :server_version, :publish_error, :publish_fail_at, :responder

    def initialize
      @published = []
      @requests = []
      @server_version = "2.12.0"
      @publish_fail_at = 0
    end

    def server_info
      {version: server_version}
    end

    def publish_msg(msg)
      @published << msg
      raise publish_error if publish_error && @published.size >= publish_fail_at
    end

    def request_msg(msg, **_opts)
      @requests << msg
      responder.call(msg)
    end
  end

  let(:nc) { FakeConnection.new }
  let(:js) { NATS::JetStream.new(nc) }
  let(:messages) do
    [
      {subject: "atomic.1", payload: "one"},
      {subject: "atomic.2", payload: "two"},
      {subject: "atomic.3", payload: "three"}
    ]
  end

  # ack builds the commit ack of the server for the request it is given.
  def ack(request, **fields)
    NATS::Msg.new(data: {
      stream: "ATOMIC",
      seq: 42,
      batch: request.header[NATS::JetStream::Header::BATCH_ID],
      count: 3
    }.merge(fields).to_json)
  end

  def batch_id_of(msg)
    msg.header[NATS::JetStream::Header::BATCH_ID]
  end

  it "publishes the messages as one atomic batch and returns the commit ack" do
    nc.responder = ->(request) { ack(request) }

    result = js.publish_batch(messages, stream: "ATOMIC")

    # All but the commit go without a reply; only the commit is a request.
    expect(nc.published.size).to eql(2)
    expect(nc.requests.size).to eql(1)
    sent = nc.published + nc.requests

    batch_id = batch_id_of(nc.requests.first)
    expect(batch_id.length).to be <= 64
    sent.each_with_index do |msg, i|
      expect(msg.subject).to eql("atomic.#{i + 1}")
      expect(msg.data).to eql(messages[i][:payload])
      expect(batch_id_of(msg)).to eql(batch_id)
      expect(msg.header[NATS::JetStream::Header::BATCH_SEQUENCE]).to eql((i + 1).to_s)
      expect(msg.header[NATS::JetStream::Header::EXPECTED_STREAM]).to eql("ATOMIC")
    end
    nc.published.each do |msg|
      expect(msg.header).not_to have_key(NATS::JetStream::Header::BATCH_COMMIT)
    end
    expect(nc.requests.first.header[NATS::JetStream::Header::BATCH_COMMIT]).to eql("1")

    expect(result.stream).to eql("ATOMIC")
    expect(result.seq).to eql(42)
    expect(result.batch).to eql(batch_id)
    expect(result.count).to eql(3)
    expect(result.duplicate).to be_falsey
  end

  it "commits a batch of a single message" do
    nc.responder = ->(request) { ack(request, count: 1) }

    result = js.publish_batch([{subject: "atomic.1", payload: "one"}])

    expect(nc.published).to be_empty
    expect(nc.requests.size).to eql(1)
    request = nc.requests.first
    expect(request.header[NATS::JetStream::Header::BATCH_SEQUENCE]).to eql("1")
    expect(request.header[NATS::JetStream::Header::BATCH_COMMIT]).to eql("1")
    expect(result.count).to eql(1)
  end

  it "returns the ack of an already committed batch as a duplicate" do
    nc.responder = ->(request) { ack(request, duplicate: true) }

    result = js.publish_batch(messages)

    expect(result.duplicate).to be(true)
    expect(result.batch).to eql(batch_id_of(nc.requests.first))
    expect(result.count).to eql(3)
  end

  it "raises BatchCommitUnknown when the ack count does not match the batch" do
    nc.responder = ->(request) { ack(request, count: 2) }

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BatchCommitUnknown) { |e|
      expect(e.batch).to eql(batch_id_of(nc.requests.first))
      expect(e.count).to eql(3)
    }
  end

  it "raises BatchCommitUnknown when the ack is for another batch" do
    nc.responder = ->(request) { ack(request, batch: "another-batch") }

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BatchCommitUnknown) { |e|
      expect(e.batch).to eql(batch_id_of(nc.requests.first))
      expect(e.count).to eql(3)
    }
  end

  it "raises BatchCommitUnknown when the connection is lost after the commit was sent" do
    nc.responder = ->(_request) { raise NATS::Timeout.new("nats: timeout") }

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BatchCommitUnknown) { |e|
      # The batch may be stored: the error carries its id, so that the
      # caller can check the stream instead of sending it again.
      expect(e.batch).to eql(batch_id_of(nc.requests.first))
      expect(e.count).to eql(3)
    }
    # The client does not retry with another batch id on its own.
    expect(nc.requests.size).to eql(1)
  end

  it "raises BatchNotCommitted when sending fails before the commit, and can be retried" do
    nc.publish_error = NATS::IO::ConnectionClosedError.new
    nc.publish_fail_at = 2

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BatchNotCommitted) { |e|
      expect(e.cause).to be_a(NATS::IO::ConnectionClosedError)
    }
    expect(nc.requests).to be_empty

    # Nothing was committed, so sending the batch again is safe.
    nc.publish_error = nil
    nc.published.clear
    nc.responder = ->(request) { ack(request) }
    result = js.publish_batch(messages)
    expect(result.count).to eql(3)
  end

  it "raises BatchNotCommitted when the commit cannot be sent" do
    nc.responder = ->(_request) { raise NATS::IO::ConnectionClosedError.new }

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BatchNotCommitted)
  end

  it "raises the API error when the stream refuses the batch" do
    nc.responder = lambda do |_request|
      NATS::Msg.new(data: {error: {code: 400, err_code: 10174, description: "atomic batches not enabled"}}.to_json)
    end

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::BadRequest) { |e|
      expect(e.err_code).to eql(10174)
      expect(e.description).to eql("atomic batches not enabled")
    }
  end

  it "raises NoStreamResponse when no stream takes the subjects" do
    nc.responder = ->(_request) { raise NATS::IO::NoRespondersError.new }

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error::NoStreamResponse)
  end

  it "refuses an empty batch before anything is sent" do
    expect { js.publish_batch([]) }.to raise_error(ArgumentError, /empty batch/)
    expect(nc.published + nc.requests).to be_empty
  end

  it "refuses a batch over the max batch size before anything is sent" do
    big = (1..257).map { |i| {subject: "atomic.#{i}", payload: "x"} }

    expect { js.publish_batch(big) }.to raise_error(ArgumentError, /max batch size/)
    expect { js.publish_batch(messages, max_batch_size: 2) }.to raise_error(ArgumentError, /max batch size/)
    expect(nc.published + nc.requests).to be_empty
  end

  it "refuses a message without a subject before anything is sent" do
    messages << {subject: "", payload: "four"}

    expect { js.publish_batch(messages) }.to raise_error(ArgumentError, /invalid subject/)
    expect(nc.published + nc.requests).to be_empty
  end

  it "refuses a server known to be too old before anything is sent" do
    nc.server_version = "2.11.4"

    expect { js.publish_batch(messages) }.to raise_error(NATS::JetStream::Error, /2.12.0/)
    expect(nc.published + nc.requests).to be_empty
  end
end
