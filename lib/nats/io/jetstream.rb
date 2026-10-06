# frozen_string_literal: true

# Copyright 2021 The NATS Authors
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
require_relative "kv"
require_relative "jetstream/api"
require_relative "jetstream/errors"
require_relative "jetstream/header"
require_relative "jetstream/js"
require_relative "jetstream/manager"
require_relative "jetstream/msg"
require_relative "jetstream/pull_subscription"
require_relative "jetstream/push_subscription"

module NATS
  # JetStream returns a context with a similar API as the NATS::Client
  # but with enhanced functions to persist and consume messages from
  # the NATS JetStream engine.
  #
  # @example
  #   nc = NATS.connect("demo.nats.io")
  #   js = nc.jetstream()
  #
  class JetStream
    attr_reader :opts, :prefix, :nc

    # Create a new JetStream context for a NATS connection.
    #
    # @param conn [NATS::Client]
    # @param params [Hash] Options to customize JetStream context.
    # @option params [String] :prefix JetStream API prefix to use for the requests.
    # @option params [String] :domain JetStream Domain to use for the requests.
    # @option params [Float] :timeout Default timeout to use for JS requests.
    def initialize(conn, params = {})
      @nc = conn
      @prefix = if params[:prefix]
        params[:prefix]
      elsif params[:domain]
        "$JS.#{params[:domain]}.API"
      else
        JS::DefaultAPIPrefix
      end
      @opts = params
      @opts[:timeout] ||= 5 # seconds
      params[:prefix] = @prefix

      # Include JetStream::Manager
      extend Manager
      extend KeyValue::Manager
    end

    # PubAck is the API response from a successfully published message.
    #
    # @!attribute [stream] stream
    #   @return [String] Name of the stream that processed the published message.
    # @!attribute [seq] seq
    #   @return [Fixnum] Sequence of the message in the stream.
    # @!attribute [duplicate] duplicate
    #   @return [Boolean] Indicates whether the published message is a duplicate.
    # @!attribute [domain] domain
    #   @return [String] JetStream Domain that processed the ack response.
    # @!attribute [val] val
    #   @return [String] Value of the counter after the increment, set when publishing to a stream with counters enabled, unless the message was a duplicate.
    # @!attribute [batch] batch
    #   @return [String] ID of the batch, set on the ack of an atomic batch commit.
    # @!attribute [count] count
    #   @return [Integer] Number of messages the batch stored, set on the ack of an atomic batch commit.
    PubAck = Struct.new(:stream, :seq, :duplicate, :domain, :val, :batch, :count,
      keyword_init: true) do
      # Fields added by newer servers are ignored, so that they cannot break publish.
      def initialize(opts = {})
        super(**opts.slice(*members))
      end
    end

    # publish produces a message for JetStream.
    #
    # @param subject [String] The subject from a stream where the message will be sent.
    # @param payload [String] The payload of the message.
    # @param params [Hash] Options to customize the publish message request.
    # @option params [Float] :timeout Time to wait for an PubAck response or an error.
    # @option params [Hash] :header NATS Headers to use for the message; the
    #   options below replace those that they set.
    # @option params [String] :stream Expected Stream to which the message is being published.
    # @option params [Integer, Symbol] :ttl Seconds after which the stream
    #   removes the message, from 1 to 2**32, or :never to keep it past the max_age of
    #   the stream. The stream needs allow_msg_ttl (requires nats-server v2.11.0).
    # @option params [Hash] :schedule Makes the message a schedule, which
    #   publishes it to the subject given as :target: once at the Time given
    #   as :at; every so many whole seconds, given as :every; or on a :cron
    #   expression with seconds, such as "0 30 * * * *", or "@hourly",
    #   "@daily", "@weekly", "@monthly" or "@yearly", in UTC or the IANA
    #   :time_zone, which only cron schedules take. :at is sent to the
    #   nanosecond. :source publishes the last message of that subject
    #   instead, :ttl gives the published messages a TTL, like the ttl
    #   option, and rollup: true rolls up the target with each. The target
    #   and source have to be subjects of the stream, which needs
    #   allow_msg_schedules, and allow_msg_ttl for :ttl. Another schedule
    #   on the subject replaces the schedule, and deleting its message stops
    #   it. Requires nats-server v2.12.0 for :at, :target and :ttl, and
    #   v2.14.0 for the others; v2.12 ignores :source and :rollup.
    # @raise [NATS::Timeout] When it takes too long to receive an ack response.
    # @raise [ArgumentError] When an option is invalid, before the message is sent.
    # @raise [NATS::JetStream::Error::APIError] When the stream refuses the
    #   message, as a stream that does not allow TTLs refuses one with a TTL.
    # @raise [NATS::JetStream::Error::NoStreamResponse] When no stream takes
    #   the subject.
    # @return [PubAck] The pub ack response.
    def publish(subject, payload = "", **params)
      params[:timeout] ||= @opts[:timeout]
      # The options add to a copy of the header, which the caller may reuse.
      options = {
        Header::EXPECTED_STREAM => (params[:stream] if params[:stream]),
        Header::MSG_TTL => (msg_ttl(params[:ttl]) if params[:ttl])
      }.compact
      if (schedule = params[:schedule])
        raise ArgumentError.new("nats: invalid schedule #{schedule.inspect}, expected a Hash") unless schedule.is_a?(Hash)

        options.merge!(schedule_header(**schedule))
      end
      header = options.empty? ? params[:header] : params[:header].to_h.merge(options)

      # Send message with headers.
      msg = NATS::Msg.new(subject: subject,
        data: payload,
        header: header)

      begin
        resp = @nc.request_msg(msg, **params)
        result = JSON.parse(resp.data, symbolize_names: true)
      rescue ::NATS::IO::NoRespondersError
        raise JetStream::Error::NoStreamResponse.new("nats: no response from stream")
      end
      raise JS.from_error(result[:error]) if result[:error]

      PubAck.new(result)
    end

    # The most messages an atomic batch can have, the limit of the server.
    MAX_BATCH_SIZE = 256

    # The oldest server version that stores atomic batches, as
    # [major, minor, patch].
    MIN_ATOMIC_BATCH_SERVER_VERSION = [2, 12, 0].freeze

    # publish_batch publishes an ordered list of messages as one atomic
    # batch: either the stream stores them all, or it stores none of them
    # (requires nats-server v2.12.0, and a stream with allow_atomic).
    #
    # @param messages [Array<Hash>] The messages, in order, each a Hash with
    #   a :subject, an optional :payload ("" by default) and an optional
    #   :header, which the batch headers replace.
    # @param params [Hash] Options to customize the batch.
    # @option params [Float] :timeout Time to wait for the commit ack.
    # @option params [String] :batch_id Id of the batch, of up to 64
    #   characters, a new one by default. Pass the batch_id of a
    #   BatchCommitUnknown error to retry its commit: the server commits a
    #   batch id only once, so this is the only safe retry.
    # @raise [ArgumentError] When the batch is empty, has more than
    #   MAX_BATCH_SIZE messages, or has an invalid entry or :batch_id,
    #   before anything is sent.
    # @raise [NATS::JetStream::Error::BatchNotSupported] When the server is
    #   known to be too old for atomic batches, before anything is sent.
    # @raise [NATS::JetStream::Error::BatchNotCommitted] When sending failed
    #   before the commit message; the batch was not committed.
    # @raise [NATS::JetStream::Error::APIError] When the stream refuses the
    #   batch, as a stream without allow_atomic does.
    # @raise [NATS::JetStream::Error::NoStreamResponse] When no stream takes
    #   the subjects.
    # @raise [NATS::JetStream::Error::BatchCommitUnknown] When the outcome
    #   of the commit is unknown, as the connection was lost after the
    #   commit message was sent. Do not retry with a new batch id, which
    #   can store the messages twice; retry the commit with its batch_id.
    # @raise [NATS::JetStream::Error::BatchAckMismatch] When the commit ack
    #   names another batch or another count than the messages sent.
    # @return [PubAck] The commit ack, with the batch id, the number of
    #   messages stored and the stream sequence of the last one. Its
    #   duplicate is true when the batch id was already committed.
    def publish_batch(messages, **params)
      timeout = params[:timeout] || @opts[:timeout]
      entries = batch_entries(messages)
      batch_id = params[:batch_id] || NATS::NUID.new.next
      unless batch_id.is_a?(String) && batch_id.bytesize.between?(1, 64)
        raise ArgumentError.new("nats: invalid batch_id #{batch_id.inspect}, expected a string of up to 64 characters")
      end
      check_atomic_batch_server_version!

      commit = nil
      entries.each_with_index do |(subject, payload, header), i|
        header = (header || {}).merge(
          Header::BATCH_ID => batch_id,
          Header::BATCH_SEQUENCE => (i + 1).to_s
        )
        if i == entries.size - 1
          header[Header::BATCH_COMMIT] = "1"
          commit = NATS::Msg.new(subject: subject, data: payload, header: header)
        else
          begin
            @nc.publish_msg(NATS::Msg.new(subject: subject, data: payload, header: header))
          rescue NATS::IO::Error => e
            raise Error::BatchNotCommitted.new(
              "nats: batch #{batch_id} was not committed, its message #{i + 1} failed to send: #{e.message}",
              batch_id: batch_id
            )
          end
        end
      end

      begin
        resp = @nc.request_msg(commit, timeout: timeout)
      rescue ::NATS::IO::NoRespondersError
        raise JetStream::Error::NoStreamResponse.new("nats: no response from stream")
      rescue NATS::IO::Error => e
        raise Error::BatchCommitUnknown.new(
          "nats: outcome of batch #{batch_id} is unknown, its commit was sent but #{e.message}; " \
          "retry the commit with batch_id: #{batch_id.inspect}, never a new batch id",
          batch_id: batch_id
        )
      end
      result = JSON.parse(resp.data, symbolize_names: true)
      raise JS.from_error(result[:error]) if result[:error]

      ack = PubAck.new(result)
      unless ack.batch == batch_id && ack.count == entries.size
        raise Error::BatchAckMismatch.new(
          "nats: commit ack #{result.inspect} does not confirm batch #{batch_id} of #{entries.size} messages",
          batch_id: batch_id,
          ack: ack
        )
      end
      ack
    end

    # subscribe binds or creates a push subscription to a JetStream pull consumer.
    #
    # @param subject [String, Array] Subject(s) from which the messages will be fetched.
    # @param params [Hash] Options to customize the PushSubscription.
    # @option params [String] :stream Name of the Stream to which the consumer belongs.
    # @option params [String] :consumer Name of the Consumer to which the PushSubscription will be bound.
    # @option params [String] :name Name of the Consumer to which the PushSubscription will be bound.
    # @option params [String] :durable Consumer durable name from where the messages will be fetched.
    # @option params [Hash] :config Configuration for the consumer.
    # @return [NATS::JetStream::PushSubscription]
    def subscribe(subject, params = {}, &cb)
      params[:consumer] ||= params[:durable]
      params[:consumer] ||= params[:name]
      multi_filter = if subject.is_a?(Array) && (subject.size == 1)
        subject = subject.first
        false
      elsif subject.is_a?(Array) && (subject.size > 1)
        true
      end

      stream = if params[:stream].nil?
        if multi_filter
          # Use the first subject to try to find the stream.
          streams = subject.map do |s|
            find_stream_name_by_subject(s)
          rescue NATS::JetStream::Error::NotFound
            raise NATS::JetStream::Error.new("nats: could not find stream matching filter subject '#{s}'")
          end

          # Ensure that the filter subjects are not ambiguous.
          streams.uniq!
          if streams.count > 1
            raise NATS::JetStream::Error.new("nats: multiple streams matched filter subjects: #{streams}")
          end

          streams.first
        else
          find_stream_name_by_subject(subject)
        end
      else
        params[:stream]
      end

      queue = params[:queue]
      durable = params[:durable]
      params[:flow_control]
      manual_ack = params[:manual_ack]
      idle_heartbeat = params[:idle_heartbeat]
      flow_control = params[:flow_control]
      config = params[:config]

      if queue
        if durable && (durable != queue)
          raise NATS::JetStream::Error.new("nats: cannot create queue subscription '#{queue}' to consumer '#{durable}'")
        else
          durable = queue
        end
      end

      cinfo = nil
      consumer_found = false
      should_create = false

      if !durable
        should_create = true
      else
        begin
          cinfo = consumer_info(stream, durable)
          config = cinfo.config
          consumer_found = true
          consumer = durable
        rescue NATS::JetStream::Error::NotFound
          should_create = true
          consumer_found = false
        end
      end

      if consumer_found
        if !config.deliver_group
          if queue
            raise NATS::JetStream::Error.new("nats: cannot create a queue subscription for a consumer without a deliver group")
          elsif cinfo.push_bound
            raise NATS::JetStream::Error.new("nats: consumer is already bound to a subscription")
          end
        elsif !queue
          raise NATS::JetStream::Error.new("nats: cannot create a subscription for a consumer with a deliver group #{config.deliver_group}")
        elsif queue != config.deliver_group
          raise NATS::JetStream::Error.new("nats: cannot create a queue subscription #{queue} for a consumer with a deliver group #{config.deliver_group}")
        end
      elsif should_create
        # Auto-create consumer if none found.
        if config.nil?
          # Defaults
          config = JetStream::API::ConsumerConfig.new({ack_policy: "explicit"})
        elsif config.is_a?(Hash)
          config = JetStream::API::ConsumerConfig.new(config)
        elsif !config.is_a?(JetStream::API::ConsumerConfig)
          raise NATS::JetStream::Error.new("nats: invalid ConsumerConfig")
        end

        config.durable_name = durable if !config.durable_name
        config.deliver_group = queue if !config.deliver_group

        # Create inbox for push consumer.
        deliver = @nc.new_inbox
        config.deliver_subject = deliver

        # Auto created consumers use the filter subject.
        if multi_filter
          config[:filter_subjects] ||= subject
        else
          config[:filter_subject] ||= subject
        end

        # Heartbeats / FlowControl
        config.flow_control = flow_control
        if idle_heartbeat || config.idle_heartbeat
          idle_heartbeat = config.idle_heartbeat if config.idle_heartbeat
          config.idle_heartbeat = idle_heartbeat
        end

        # Auto create the consumer.
        cinfo = add_consumer(stream, config)
        consumer = cinfo.name
      end

      # Enable auto acking for async callbacks unless disabled.
      # In case ack policy is none then we also do not require to ack.
      if cb && !manual_ack && (config.ack_policy != "none")
        ocb = cb
        new_cb = proc do |msg|
          ocb.call(msg)
          begin
            msg.ack
          rescue
            JetStream::Error::MsgAlreadyAckd
          end
        end
        cb = new_cb
      end
      sub = @nc.subscribe(config.deliver_subject, queue: config.deliver_group, &cb)
      sub.extend(PushSubscription)
      sub.jsi = JS::Sub.new(
        js: self,
        stream: stream,
        consumer: consumer
      )
      sub
    end

    # pull_subscribe binds or creates a subscription to a JetStream pull consumer.
    #
    # @param subject [String, Array] Subject or subjects from which the messages will be fetched.
    # @param durable [String] Consumer durable name from where the messages will be fetched.
    # @param params [Hash] Options to customize the PullSubscription.
    # @option params [String] :stream Name of the Stream to which the consumer belongs.
    # @option params [String] :consumer Name of the Consumer to which the PullSubscription will be bound.
    # @option params [String] :name Name of the Consumer to which the PullSubscription will be bound.
    # @option params [Hash] :config Configuration for the consumer.
    # @return [NATS::JetStream::PullSubscription]
    def pull_subscribe(subject, durable, params = {})
      if (!durable || durable.empty?) && !(params[:consumer] || params[:name])
        raise JetStream::Error::InvalidDurableName.new("nats: invalid durable name")
      end
      multi_filter = if subject.is_a?(Array) && (subject.size == 1)
        subject = subject.first
        false
      elsif subject.is_a?(Array) && (subject.size > 1)
        true
      end

      params[:consumer] ||= durable
      params[:consumer] ||= params[:name]
      stream = if params[:stream].nil?
        if multi_filter
          # Use the first subject to try to find the stream.
          streams = subject.map do |s|
            find_stream_name_by_subject(s)
          rescue NATS::JetStream::Error::NotFound
            raise NATS::JetStream::Error.new("nats: could not find stream matching filter subject '#{s}'")
          end

          # Ensure that the filter subjects are not ambiguous.
          streams.uniq!
          if streams.count > 1
            raise NATS::JetStream::Error.new("nats: multiple streams matched filter subjects: #{streams}")
          end

          streams.first
        else
          find_stream_name_by_subject(subject)
        end
      else
        params[:stream]
      end
      begin
        consumer_info(stream, params[:consumer])
      rescue NATS::JetStream::Error::NotFound => e
        # If attempting to bind, then this is a hard error.
        raise e if params[:stream] && !multi_filter

        config = if !params[:config]
          JetStream::API::ConsumerConfig.new
        elsif params[:config].is_a?(JetStream::API::ConsumerConfig)
          params[:config]
        else
          JetStream::API::ConsumerConfig.new(params[:config])
        end
        config[:durable_name] = durable
        config[:ack_policy] ||= JS::Config::AckExplicit
        if multi_filter
          config[:filter_subjects] ||= subject
        else
          config[:filter_subject] ||= subject
        end
        add_consumer(stream, config)
      end

      deliver = @nc.new_inbox
      sub = @nc.subscribe(deliver)
      sub.extend(PullSubscription)

      consumer = params[:consumer]
      subject = "#{@prefix}.CONSUMER.MSG.NEXT.#{stream}.#{consumer}"
      sub.jsi = JS::Sub.new(
        js: self,
        stream: stream,
        consumer: params[:consumer],
        nms: subject
      )
      sub
    end

    private

    # msg_ttl formats a message TTL as the server takes it. Longer TTLs than
    # 2**32 seconds, some 136 years, overflow in the server, which then
    # removes the message at once.
    def msg_ttl(ttl)
      return "never" if ttl == :never
      unless ttl.is_a?(Integer) && ttl.between?(1, 2**32)
        raise ArgumentError.new("nats: invalid ttl #{ttl.inspect}, expected whole seconds from 1 to 2**32, or :never")
      end

      ttl.to_s
    end

    # schedule_header makes the headers of a message schedule.
    def schedule_header(target:, at: nil, every: nil, cron: nil, source: nil, ttl: nil, time_zone: nil, rollup: nil)
      pattern = if at.is_a?(Time)
        "@at #{at.getutc.iso8601(9)}"
      elsif every.is_a?(Integer) && every >= 1
        "@every #{every}s"
      elsif nonempty_string?(cron)
        cron
      end
      unless pattern && [at, every, cron].compact.size == 1
        raise ArgumentError.new("nats: a schedule needs one of at: a Time, every: whole seconds from 1, or cron: an expression")
      end
      raise ArgumentError.new("nats: a schedule needs a target subject") unless nonempty_string?(target)
      raise ArgumentError.new("nats: invalid schedule source #{source.inspect}") unless source.nil? || nonempty_string?(source)
      raise ArgumentError.new("nats: invalid schedule time zone #{time_zone.inspect}") unless time_zone.nil? || nonempty_string?(time_zone)
      raise ArgumentError.new("nats: only cron: schedules take a time zone") if time_zone && !cron
      raise ArgumentError.new("nats: invalid schedule rollup #{rollup.inspect}") unless [nil, true, false].include?(rollup)

      {
        Header::SCHEDULE => pattern,
        Header::SCHEDULE_TARGET => target,
        Header::SCHEDULE_SOURCE => source,
        Header::SCHEDULE_TTL => (msg_ttl(ttl) if ttl),
        Header::SCHEDULE_TIME_ZONE => time_zone,
        Header::SCHEDULE_ROLLUP => ("sub" if rollup)
      }.compact
    end

    def nonempty_string?(value)
      value.is_a?(String) && !value.empty?
    end

    # batch_entries validates the entries of an atomic batch and returns
    # them as [subject, payload, header] triples, before anything is sent.
    def batch_entries(messages)
      unless messages.is_a?(Array) && !messages.empty?
        raise ArgumentError.new("nats: invalid batch #{messages.inspect}, expected a non-empty Array of {subject:, payload:} Hashes")
      end
      if messages.size > MAX_BATCH_SIZE
        raise ArgumentError.new("nats: batch of #{messages.size} messages exceeds the maximum of #{MAX_BATCH_SIZE}")
      end
      messages.map do |entry|
        unless entry.is_a?(Hash) && nonempty_string?(entry[:subject])
          raise ArgumentError.new("nats: invalid batch entry #{entry.inspect}, expected a Hash with a :subject")
        end
        payload = entry[:payload] || ""
        unless payload.is_a?(String)
          raise ArgumentError.new("nats: invalid batch entry payload #{payload.inspect}, expected a String")
        end
        header = entry[:header]
        unless header.nil? || header.is_a?(Hash)
          raise ArgumentError.new("nats: invalid batch entry header #{header.inspect}, expected a Hash")
        end
        [entry[:subject], payload, header]
      end
    end

    # check_atomic_batch_server_version! refuses atomic batches before
    # anything is sent when the server is known to be too old for them.
    def check_atomic_batch_server_version!
      version = @nc.server_info[:version]
      return unless version.is_a?(String)
      match = version.match(/^\d+\.\d+\.\d+/)
      return unless match
      if (match[0].split(".").map(&:to_i) <=> MIN_ATOMIC_BATCH_SERVER_VERSION) < 0
        required = MIN_ATOMIC_BATCH_SERVER_VERSION.join(".")
        raise Error::BatchNotSupported.new("nats: atomic batch publish requires nats-server v#{required} or later, server is #{version}")
      end
    end
  end
end
