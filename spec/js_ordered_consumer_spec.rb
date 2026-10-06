# frozen_string_literal: true

require "spec_helper"

# Records the subscriptions and published messages of an OrderedConsumer,
# so that the examples can drive a controlled flow of messages, heartbeats,
# flow control requests and late deliveries of replaced consumers.
class ControlledConnection
  Subscription = Struct.new(:subject, :callback, :unsubscribed) do
    def unsubscribe
      self.unsubscribed = true
    end
  end

  attr_reader :subscriptions, :published

  def initialize
    @subscriptions = []
    @published = []
    @inbox_seq = 0
  end

  def new_inbox
    @inbox_seq += 1
    "_INBOX.#{@inbox_seq}"
  end

  def subscribe(subject, _opts = {}, &callback)
    Subscription.new(subject, callback, false).tap { |sub| @subscriptions << sub }
  end

  def publish(subject, payload = "", _reply = nil)
    @published << {subject: subject, payload: payload}
  end
end

describe "JetStream::OrderedConsumer" do
  let(:conn) { ControlledConnection.new }
  let(:consumer_configs) { [] }
  let(:consumer_errors) { {} }
  let(:js) do
    context = NATS::JetStream.new(conn)
    allow(context).to receive(:add_consumer) do |_stream, config|
      consumer_configs << config
      if (error = consumer_errors[consumer_configs.size - 1])
        raise error
      end
      double(name: "consumer-#{consumer_configs.size}")
    end
    context
  end
  let(:ordered) { js.ordered_consumer("TEST", filter_subject: "events.>") }

  def js_msg(consumer:, sseq:, dseq:)
    NATS::Msg.new(
      subject: "events.#{sseq}",
      reply: "$JS.ACK.TEST.#{consumer}.1.#{sseq}.#{dseq}.1690000000000000000.0",
      data: "msg #{sseq}"
    )
  end

  def status_msg(status, desc, headers = {})
    NATS::Msg.new(
      subject: "status",
      header: {"Status" => status, "Description" => desc}.merge(headers)
    )
  end

  def heartbeat(last_consumer:, last_stream:)
    status_msg("100", "Idle Heartbeat",
      "Nats-Last-Consumer" => last_consumer.to_s,
      "Nats-Last-Stream" => last_stream.to_s)
  end

  def flow_control(reply:)
    msg = status_msg("100", "FlowControl Request")
    msg.reply = reply
    msg
  end

  # consume runs ordered.each in a thread, collecting the delivered stream
  # sequences into seen, and stopping once seen has count of them.
  def consume(ordered, seen, count:)
    Thread.new do
      ordered.each do |msg|
        seen << msg.metadata.sequence.stream
        ordered.stop if seen.size >= count
      end
    end
  end

  it "requires a stream name" do
    expect { js.ordered_consumer("") }.to raise_error(NATS::JetStream::Error::InvalidStreamName)
  end

  it "requires a block" do
    expect { ordered.each }.to raise_error(NATS::JetStream::Error, /requires a block/)
  end

  it "delivers messages in stream order, where stream sequence gaps from the filter subject are not losses" do
    seen = []
    reader = consume(ordered, seen, count: 3)
    sub = wait_until { conn.subscriptions[0] }

    config = consumer_configs[0]
    expect(config.ack_policy).to eql("none")
    expect(config.deliver_policy).to eql("all")
    expect(config.filter_subject).to eql("events.>")
    expect(config.flow_control).to be(true)
    expect(config.mem_storage).to be(true)

    sub.callback.call(js_msg(consumer: "consumer-1", sseq: 1, dseq: 1))
    sub.callback.call(js_msg(consumer: "consumer-1", sseq: 5, dseq: 2))
    sub.callback.call(js_msg(consumer: "consumer-1", sseq: 9, dseq: 3))

    expect(reader.join(5)).to be(reader)
    expect(seen).to eql([1, 5, 9])
    expect(consumer_configs.size).to eql(1)
  end

  it "recreates the consumer from after the last delivered stream sequence when the consumer sequence skips" do
    seen = []
    reader = consume(ordered, seen, count: 4)
    first = wait_until { conn.subscriptions[0] }
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 1, dseq: 1))
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 2, dseq: 2))
    # The delivery with consumer sequence 3 never arrives.
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 4, dseq: 4))

    second = wait_until { conn.subscriptions[1] }
    config = consumer_configs[1]
    expect(config.deliver_policy).to eql("by_start_sequence")
    expect(config.opt_start_seq).to eql(3)
    expect(first.unsubscribed).to be(true)

    second.callback.call(js_msg(consumer: "consumer-2", sseq: 3, dseq: 1))
    second.callback.call(js_msg(consumer: "consumer-2", sseq: 4, dseq: 2))

    expect(reader.join(5)).to be(reader)
    expect(seen).to eql([1, 2, 3, 4])
  end

  it "recreates the consumer when an idle heartbeat reports deliveries that never arrived" do
    seen = []
    reader = consume(ordered, seen, count: 3)
    first = wait_until { conn.subscriptions[0] }
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 1, dseq: 1))
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 2, dseq: 2))

    # Up to date: no recreation.
    first.callback.call(heartbeat(last_consumer: 2, last_stream: 2))
    expect(consumer_configs.size).to eql(1)

    # The consumer delivered a message that never arrived.
    first.callback.call(heartbeat(last_consumer: 3, last_stream: 3))
    second = wait_until { conn.subscriptions[1] }
    expect(consumer_configs[1].opt_start_seq).to eql(3)

    second.callback.call(js_msg(consumer: "consumer-2", sseq: 3, dseq: 1))

    expect(reader.join(5)).to be(reader)
    expect(seen).to eql([1, 2, 3])
  end

  it "answers flow control requests without delivering them or recreating the consumer" do
    seen = []
    reader = consume(ordered, seen, count: 1)
    first = wait_until { conn.subscriptions[0] }

    first.callback.call(flow_control(reply: "$JS.FC.TEST.1"))
    expect(conn.published).to eql([{subject: "$JS.FC.TEST.1", payload: ""}])

    first.callback.call(js_msg(consumer: "consumer-1", sseq: 1, dseq: 1))

    expect(reader.join(5)).to be(reader)
    expect(seen).to eql([1])
    expect(consumer_configs.size).to eql(1)
  end

  it "drops late messages and control frames of a replaced consumer generation" do
    seen = []
    reader = consume(ordered, seen, count: 4)
    first = wait_until { conn.subscriptions[0] }
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 1, dseq: 1))
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 2, dseq: 2))
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 4, dseq: 4))
    second = wait_until { conn.subscriptions[1] }

    # A tail delivery of the replaced consumer arrives late, as does a
    # heartbeat of its generation; neither may deliver or recover again.
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 3, dseq: 3))
    first.callback.call(heartbeat(last_consumer: 9, last_stream: 9))
    expect(consumer_configs.size).to eql(2)

    second.callback.call(js_msg(consumer: "consumer-2", sseq: 3, dseq: 1))
    second.callback.call(js_msg(consumer: "consumer-2", sseq: 4, dseq: 2))

    expect(reader.join(5)).to be(reader)
    expect(seen).to eql([1, 2, 3, 4])
  end

  it "runs a single recovery when losses are noticed concurrently" do
    seen = []
    reader = consume(ordered, seen, count: 1)
    wait_until { conn.subscriptions[0] }

    gap = js_msg(consumer: "consumer-1", sseq: 3, dseq: 3)
    threads = 4.times.map { Thread.new { ordered.send(:process, gap, 1) } }
    threads.each(&:join)

    second = wait_until { conn.subscriptions[1] }
    expect(consumer_configs.size).to eql(2)

    second.callback.call(js_msg(consumer: "consumer-2", sseq: 3, dseq: 1))

    expect(reader.join(5)).to be(reader)
    expect(seen).to eql([3])
  end

  it "recreates the consumer when the server reports it deleted" do
    seen = []
    reader = consume(ordered, seen, count: 2)
    first = wait_until { conn.subscriptions[0] }
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 1, dseq: 1))
    first.callback.call(status_msg("409", "Consumer Deleted"))

    second = wait_until { conn.subscriptions[1] }
    expect(consumer_configs[1].opt_start_seq).to eql(2)

    second.callback.call(js_msg(consumer: "consumer-2", sseq: 2, dseq: 1))

    expect(reader.join(5)).to be(reader)
    expect(seen).to eql([1, 2])
  end

  it "stops and raises the error of the block, not counting its message as delivered" do
    seen = []
    error = nil
    reader = Thread.new do
      ordered.each do |msg|
        sseq = msg.metadata.sequence.stream
        raise "analysis failed" if sseq == 2

        seen << sseq
      end
    rescue => e
      error = e
    end
    first = wait_until { conn.subscriptions[0] }
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 1, dseq: 1))
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 2, dseq: 2))
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 3, dseq: 3))

    expect(reader.join(5)).to be(reader)
    expect(error&.message).to eql("analysis failed")
    expect(seen).to eql([1])
    expect(ordered.delivered).to eql(1)
    expect(first.unsubscribed).to be(true)
  end

  it "ends the wait with the error when the consumer cannot be recreated" do
    consumer_errors[1] = NATS::JetStream::Error::StreamNotFound.new
    error = nil
    reader = Thread.new do
      ordered.each { nil }
    rescue => e
      error = e
    end
    first = wait_until { conn.subscriptions[0] }
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 1, dseq: 1))
    first.callback.call(js_msg(consumer: "consumer-1", sseq: 3, dseq: 3))

    expect(reader.join(5)).to be(reader)
    expect(error).to be_a(NATS::JetStream::Error::StreamNotFound)
  end

  it "ends the wait with the error of a terminal status" do
    error = nil
    reader = Thread.new do
      ordered.each { nil }
    rescue => e
      error = e
    end
    first = wait_until { conn.subscriptions[0] }
    first.callback.call(status_msg("503", "Service Unavailable"))

    expect(reader.join(5)).to be(reader)
    expect(error).to be_a(NATS::JetStream::Error::ServiceUnavailable)
  end

  it "raises when the first consumer cannot be created" do
    consumer_errors[0] = NATS::JetStream::Error::StreamNotFound.new
    expect { ordered.each { nil } }.to raise_error(NATS::JetStream::Error::StreamNotFound)
  end
end
