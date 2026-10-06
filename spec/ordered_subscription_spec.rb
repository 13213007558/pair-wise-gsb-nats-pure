# frozen_string_literal: true

require "nats/io/jetstream"

FakeOrderedConsumerInfo = Struct.new(:stream_name, :name, :config)

describe NATS::JetStream::OrderedSubscription do
  def wait_for_predicate(seconds)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
    loop do
      return if yield

      raise "condition not met" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.001
    end
  end

  class FakeOrderedSub
    attr_reader :subject
    attr_accessor :callback

    def initialize(_nc, subject)
      @subject = subject
    end

    def deliver(msg)
      callback.call(msg)
    end

    def unsubscribe; end
  end

  class FakeOrderedNC
    attr_reader :errors, :replies

    def initialize
      @inboxes = 0
      @subs = []
      @errors = []
      @replies = []
    end

    attr_reader :subs

    def new_inbox
      @inboxes += 1
      "$inbox.#{@inboxes}"
    end

    def subscribe(subject, &callback)
      FakeOrderedSub.new(self, subject).tap do |sub|
        sub.callback = callback
        @subs << sub
      end
    end

    def publish(subject, payload)
      @replies << [subject, payload]
    end

    def err_cb_call(nc, error, sub)
      @errors << [error, sub]
    end

    def current
      @subs.last
    end
  end

  class FakeOrderedJS
    attr_reader :configs, :create_count
    attr_accessor :before_create

    def initialize(nc, consumers)
      @nc = nc
      @consumers = consumers
      @configs = []
      @create_count = 0
      @before_create = nil
    end

    def nc
      @nc
    end

    def add_consumer(stream, config)
      @create_count += 1
      @configs << config
      raise @create_error if @create_error
      @before_create&.call(config, @create_count)
      @consumers.call(stream, config, @create_count)
    end

    def fail_next_create(error)
      @create_error = error
    end

    def consumer_info(stream, consumer, params = {})
      {stream: stream, consumer: consumer, params: params}
    end
  end

  let(:nc) { FakeOrderedNC.new }
  let(:js) do
    FakeOrderedJS.new(nc, proc do |stream, config, generation|
      FakeOrderedConsumerInfo.new(stream, "ordered-#{generation}", config)
    end)
  end

  def data_msg(stream_seq:, consumer_seq:, subject: "$inbox.1", reply_subject: "$JS.ACK.test.cons.1.#{stream_seq}.#{consumer_seq}.0.0", header: nil)
    NATS::Msg.new(
      subject: subject,
      reply: reply_subject,
      data: "m#{stream_seq}",
      header: header,
      nc: nc,
      sub: nc.subs.first
    )
  end

  def control_msg(kind:, last_consumer:, last_stream:, generation: 1, reply: "$JS.FC.test")
    sub = nc.subs[generation - 1]
    NATS::Msg.new(
      subject: sub.subject,
      reply: reply,
      data: "",
      header: {
        "Status" => "100",
        "Description" => kind,
        "Nats-Last-Consumer" => last_consumer.to_s,
        "Nats-Last-Stream" => last_stream.to_s
      },
      nc: nc,
      sub: sub
    )
  end

  def ordered_sub(callback = nil, **opts)
    described_class.new(
      js: js,
      stream: "ORD",
      subject: "foo",
      config: {idle_heartbeat: 3600},
      callback: callback,
      **opts
    )
  end

  after do
    @sub&.unsubscribe
  end

  it "allows filtered stream sequence gaps but recovers from a consumer sequence gap" do
    delivered = []
    @sub = ordered_sub(proc { |msg| delivered << [msg.metadata.sequence.stream, msg.data] })

    nc.current.deliver(data_msg(stream_seq: 1, consumer_seq: 1))
    nc.current.deliver(data_msg(stream_seq: 3, consumer_seq: 2))
    wait_for_predicate(0.1) { delivered.size >= 2 }

    nc.subs[0].deliver(data_msg(stream_seq: 5, consumer_seq: 4, subject: nc.subs[0].subject))
    nc.subs.last.deliver(data_msg(stream_seq: 4, consumer_seq: 1, subject: nc.subs.last.subject))
    wait_for_predicate(0.1) { delivered.size >= 3 }

    expect(delivered).to eql([[1, "m1"], [3, "m3"], [4, "m4"]])
    expect(js.create_count).to eql(2)
    expect(js.configs.last.to_h.slice(:deliver_policy, :opt_start_seq, :ack_policy, :flow_control, :filter_subject))
      .to eql({
        deliver_policy: "by_start_sequence",
        opt_start_seq: 4,
        ack_policy: "none",
        flow_control: true,
        filter_subject: "foo"
      })
  end

  it "acks only current-generation flow control and rebuilds from the last delivered stream sequence" do
    delivered = []
    @sub = ordered_sub(proc { |msg| delivered << msg.metadata.sequence.stream })
    nc.current.deliver(data_msg(stream_seq: 4, consumer_seq: 1))
    wait_for_predicate(0.1) { delivered.any? }

    nc.subs[0].deliver(control_msg(kind: "FlowControl Request", last_consumer: 1, last_stream: 4, generation: 1, reply: "$fc.current"))
    expect(nc.replies).to include(["$fc.current", ""])

    barrier = Queue.new
    release = Queue.new
    thread = Thread.new do
      barrier << :ready
      release.pop
      nc.subs[0].deliver(control_msg(kind: "FlowControl Request", last_consumer: 2, last_stream: 9, generation: 1, reply: "$fc.gap"))
    end
    barrier.pop
    release << :go
    thread.join
    wait_for_predicate(0.1) { js.create_count == 2 }

    nc.subs[0].deliver(control_msg(kind: "FlowControl Request", last_consumer: 3, last_stream: 10, generation: 1, reply: "$fc.late"))
    nc.subs.last.deliver(control_msg(kind: "FlowControl Request", last_consumer: 0, last_stream: 4, generation: 2, reply: "$fc.new"))
    expect(nc.replies).to_not include(["$fc.late", ""])
    expect(nc.replies).to include(["$fc.new", ""])
    expect(js.configs.last[:opt_start_seq]).to eql(5)
  end

  it "rebuilds when a heartbeat advertises progress beyond the delivered consumer sequence" do
    @sub = ordered_sub
    nc.current.deliver(data_msg(stream_seq: 7, consumer_seq: 1))
    expect(@sub.next_msg(timeout: 1).metadata.sequence.stream).to eql(7)

    nc.current.deliver(control_msg(kind: "Idle Heartbeat", last_consumer: 2, last_stream: 8, generation: 1))
    wait_for_predicate(0.1) { js.create_count == 2 }
    expect(js.configs.last[:opt_start_seq]).to eql(8)
  end

  it "ignores heartbeat and late data tails from the previous consumer generation" do
    delivered = Queue.new
    @sub = ordered_sub(proc { |msg| delivered << msg })
    nc.current.deliver(data_msg(stream_seq: 2, consumer_seq: 1))
    wait_for_predicate(0.1) { !delivered.empty? }
    nc.current.deliver(control_msg(kind: "Idle Heartbeat", last_consumer: 2, last_stream: 3, generation: 1))
    wait_for_predicate(0.1) { js.create_count == 2 }

    nc.subs[0].deliver(data_msg(stream_seq: 3, consumer_seq: 2, subject: nc.subs[0].subject))
    nc.subs[0].deliver(control_msg(kind: "Idle Heartbeat", last_consumer: 3, last_stream: 3, generation: 1))
    nc.subs.last.deliver(data_msg(stream_seq: 3, consumer_seq: 1, subject: nc.subs.last.subject))
    wait_for_predicate(0.1) { delivered.size >= 2 }

    sequences = []
    sequences << delivered.pop.metadata.sequence.stream until delivered.empty?
    expect(sequences).to eql([2, 3])
    expect(js.create_count).to eql(2)
  end

  it "coalesces simultaneous gap triggers into one recovery" do
    @sub = ordered_sub
    barrier = Queue.new
    release = Queue.new
    js.define_singleton_method(:add_consumer) do |stream, config|
      barrier << :ready
      release.pop
      FakeOrderedConsumerInfo.new(nil, "blocked", nil)
    end

    threads = 3.times.map do
      Thread.new { @sub.send(:schedule_recovery, 1) }
    end
    wait_for_predicate(0.1) { barrier.size == 1 }
    sleep 0.05
    release << :go
    threads.each(&:join)
    barrier.pop(true)
  end

  it "treats callback failure as non-delivery and redelivers the same stream sequence" do
    attempts = 0
    delivered = []
    @sub = ordered_sub(proc do |msg|
      attempts += 1
      raise "temporary failure" if attempts == 1

      delivered << msg.metadata.sequence.stream
    end)

    nc.current.deliver(data_msg(stream_seq: 6, consumer_seq: 1))
    wait_for_predicate(0.1) { js.create_count == 2 }
    nc.subs.last.deliver(data_msg(stream_seq: 6, consumer_seq: 1, subject: nc.subs.last.subject))
    wait_for_predicate(0.1) { delivered.any? }

    expect(delivered).to eql([6])
    expect(js.configs.last[:opt_start_seq]).to eql(6)
    expect(nc.errors.map { |error, _sub| error.message }).to include("temporary failure")
  end

  it "ends next_msg with an unrecoverable rebuild error" do
    error = StandardError.new("consumer api unavailable")
    @sub = ordered_sub
    nc.current.deliver(data_msg(stream_seq: 1, consumer_seq: 1))
    expect(@sub.next_msg(timeout: 1).data).to eql("m1")
    js.fail_next_create(error)
    nc.current.deliver(data_msg(stream_seq: 3, consumer_seq: 3))
    wait_for_predicate(0.1) { js.create_count == 2 }

    expect { @sub.next_msg(timeout: 1) }.to raise_error(error)
  end

  it "creates an ephemeral ack-none push consumer" do
    @sub = ordered_sub
    config = js.configs.first
    expect(config[:durable_name]).to be_nil
    expect(config[:name]).to be_nil
    expect(config[:deliver_subject]).to start_with("$inbox.")
    expect(config[:ack_policy]).to eql("none")
    expect(config[:max_deliver]).to eql(1)
    expect(config[:flow_control]).to be(true)
  end

  it "accepts the first message while the recreated consumer API is in flight" do
    delivered = Queue.new
    @sub = ordered_sub(proc { |msg| delivered << msg })
    nc.current.deliver(data_msg(stream_seq: 5, consumer_seq: 1))
    expect(delivered.pop.metadata.sequence.stream).to eql(5)

    ready = Queue.new
    release = Queue.new
    js.before_create = proc do |_config, generation|
      next unless generation == 2

      ready << :ready
      release.pop
    end
    trigger = Thread.new { nc.subs[0].deliver(data_msg(stream_seq: 7, consumer_seq: 3, subject: nc.subs[0].subject)) }
    ready.pop
    nc.subs.last.deliver(data_msg(stream_seq: 6, consumer_seq: 1, subject: nc.subs.last.subject))
    release << :go
    trigger.join

    expect(delivered.pop.metadata.sequence.stream).to eql(6)
  end

  it "retries when the in-flight recreated consumer's first callback fails" do
    attempts = 0
    delivered = Queue.new
    @sub = ordered_sub(proc do |msg|
      attempts += 1
      raise "first rebuild failure" if attempts == 2

      delivered << msg
    end)
    nc.current.deliver(data_msg(stream_seq: 5, consumer_seq: 1))
    expect(delivered.pop.metadata.sequence.stream).to eql(5)

    ready = Queue.new
    release = Queue.new
    js.before_create = proc do |_config, generation|
      next unless generation == 2

      ready << :ready
      release.pop
    end
    trigger = Thread.new { nc.subs[0].deliver(data_msg(stream_seq: 7, consumer_seq: 3, subject: nc.subs[0].subject)) }
    ready.pop
    nc.subs.last.deliver(data_msg(stream_seq: 6, consumer_seq: 1, subject: nc.subs.last.subject))
    release << :go
    trigger.join
    wait_for_predicate(0.1) { js.create_count == 3 }
    nc.subs.last.deliver(data_msg(stream_seq: 6, consumer_seq: 1, subject: nc.subs.last.subject))

    expect(delivered.pop.metadata.sequence.stream).to eql(6)
    expect(js.configs.last[:opt_start_seq]).to eql(6)
  end
end
