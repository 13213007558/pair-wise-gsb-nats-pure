# frozen_string_literal: true

require_relative "js/header"
require_relative "js/status"

module NATS
  class JetStream
    class OrderedSubscription
      include MonitorMixin

      StatusHeader = "Status"
      DescriptionHeader = "Description"
      IdleHeartbeat = "Idle Heartbeat"
      FlowControlRequest = "FlowControl Request"

      attr_reader :stream, :consumer, :subject

      def initialize(js:, stream:, subject:, config: {}, callback: nil, idle_heartbeat: nil)
        super()
        @js = js
        @nc = js.nc
        @stream = stream
        @filter = subject
        @callback = callback
        @config = prepare_config(config, idle_heartbeat)
        @queue = Queue.new unless @callback
        @cond = new_cond
        @generation = 0
        @sub = nil
        @consumer = nil
        @subject = nil
        @next_consumer_seq = 1
        @last_stream_seq = start_sequence - 1
        @active = true
        @recovering = false
        @recovery_failed_stream_seq = nil
        @delivering = false
        @closed = false
        @error = nil
        @hb_queue = Queue.new

        start_consumer(
          deliver_policy: @config[:deliver_policy] || (@config[:opt_start_seq] ? "by_start_sequence" : "all"),
          opt_start_seq: @config[:opt_start_seq],
          last_stream_seq: @last_stream_seq
        )
        start_heartbeat
      end

      def next_msg(params = {})
        raise ::NATS::JetStream::Error.new("nats: ordered subscription with a callback cannot use next_msg") if @callback

        timeout = params[:timeout] || 0.5
        deadline = MonotonicTime.now + timeout
        synchronize do
          loop do
            raise @error if @error
            return @queue.pop(true) unless @queue.empty?

            remaining = deadline - MonotonicTime.now
            raise ::NATS::Timeout.new("nats: timeout") if remaining <= 0

            @cond.wait(remaining)
          end
        end
      end

      def unsubscribe
        sub = synchronize do
          @closed = true
          @recovering = false
          @recovery_failed_stream_seq = nil
          @delivering = false
          @active = false
          @cond.broadcast
          @sub
        end
        stop_heartbeat
        unsubscribe_ignoring_errors(sub)
      rescue => e
        fail!(e)
      end

      def consumer_info(params = {})
        stream, consumer, sub = synchronize { [@stream, @consumer, @sub] }
        @js.consumer_info(stream, consumer, params) if consumer && sub
      end

      private

      def prepare_config(config, idle_heartbeat)
        config = if config.is_a?(JetStream::API::ConsumerConfig)
          config.to_h.dup
        else
          config.dup
        end

        [:durable_name, :name, :deliver_group].each do |key|
          if config[key]
            raise ArgumentError.new("nats: ordered consumers are ephemeral push consumers; #{key} is not supported")
          end
        end

        heartbeat = idle_heartbeat || config[:idle_heartbeat] || 5
        unless heartbeat.is_a?(Integer) && heartbeat.positive?
          raise ArgumentError.new("nats: ordered consumer idle heartbeat must be a positive integer number of seconds")
        end

        config.merge(
          ack_policy: JS::Config::AckNone,
          ack_wait: config[:ack_wait] || (22 * 3600),
          max_deliver: 1,
          flow_control: true,
          idle_heartbeat: heartbeat,
          num_replicas: 1,
          mem_storage: true,
          deliver_group: nil,
          durable_name: nil,
          name: nil,
          replay_policy: config[:replay_policy] || "instant"
        )
      end

      def start_sequence
        value = @config[:opt_start_seq]
        return 1 if value.nil?
        raise ArgumentError.new("nats: opt_start_seq must be positive") unless value.is_a?(Integer) && value.positive?

        value
      end

      def start_consumer(deliver_policy:, opt_start_seq:, last_stream_seq:)
        deliver = @nc.new_inbox
        generation = @generation + 1
        sub = nil

        synchronize do
          sub = @nc.subscribe(deliver) do |msg|
            handle_message(msg, generation)
          end
          @generation = generation
          @sub = sub
          @consumer = nil
          @subject = deliver
          @next_consumer_seq = 1
          @last_stream_seq = last_stream_seq
          @active = true
          @recovery_failed_stream_seq = nil
        end

        config = @config.merge(
          deliver_subject: deliver,
          deliver_policy: deliver_policy,
          opt_start_seq: opt_start_seq,
          filter_subject: filter_subject,
          filter_subjects: filter_subjects
        ).compact
        info = @js.add_consumer(@stream, config)

        synchronize do
          @consumer = info.name
          @cond.broadcast
        end
        info
      rescue => e
        sub&.unsubscribe
        raise
      end

      def filter_subject
        @filter.is_a?(Array) ? nil : @filter
      end

      def filter_subjects
        @filter.is_a?(Array) ? @filter : nil
      end

      def handle_message(msg, generation)
        status = msg.header&.[](StatusHeader)
        return handle_status(msg, generation) if status == JS::Status::CtrlMsg
        return fail!(JS.from_msg(msg)) if status

        meta = begin
          msg.metadata
        rescue => e
          return fail!(e)
        end
        stream_seq = meta.sequence.stream
        consumer_seq = meta.sequence.consumer

        action = synchronize do
          next [:ignore] if stale?(generation)
          next [:ignore] if @recovering && consumer_seq != @next_consumer_seq

          if consumer_seq < @next_consumer_seq || stream_seq <= @last_stream_seq
            [:ignore]
          elsif consumer_seq > @next_consumer_seq
            [:recover, generation]
          else
            @active = true
            @delivering = true
            [:deliver, msg, stream_seq]
          end
        end

        case action.first
        when :recover
          schedule_recovery(action[1])
        when :deliver
          deliver_message(action[1], action[2], generation)
        end
      end

      def handle_status(msg, generation)
        description = msg.header[DescriptionHeader].to_s
        if description.include?(FlowControlRequest)
          handle_flow_control(msg, generation)
        elsif description.include?(IdleHeartbeat)
          handle_heartbeat(msg, generation)
        else
          fail!(::NATS::JetStream::Error.new("nats: unsupported ordered consumer control message: #{description}"))
        end
      end

      def handle_heartbeat(msg, generation)
        consumer_seq = msg.header[JS::Header::LastConsumerSeq].to_i
        action = synchronize do
          next [:ignore] if stale?(generation)
          if @recovering
            @active = true
            next [:ignore]
          end

          @active = true
          case consumer_seq <=> (@next_consumer_seq - 1)
          when 0 then [:ignore]
          when -1 then [:ignore]
          else [:recover, generation]
          end
        end

        schedule_recovery(action[1]) if action.first == :recover
      end

      def handle_flow_control(msg, generation)
        consumer_seq = msg.header[JS::Header::LastConsumerSeq].to_i
        action = synchronize do
          next [:ignore] if stale?(generation)
          next [:ignore] if @recovering

          case consumer_seq <=> (@next_consumer_seq - 1)
          when 0
            @active = true
            [:ack]
          when -1
            [:ignore]
          else
            [:recover, generation]
          end
        end

        case action.first
        when :ack
          @nc.publish(msg.reply, "")
        when :recover
          schedule_recovery(action[1])
        end
      end

      def stale?(generation)
        generation != @generation || @closed || !@error.nil?
      end

      def deliver_message(msg, stream_seq, generation)
        if @callback
          begin
            @callback.call(msg)
          rescue => e
            @nc.send(:err_cb_call, @nc, e, self)
            recover_after_failed_delivery(generation, stream_seq)
            return
          end
        else
          @queue << msg
        end

        synchronize do
          next if stale?(generation)

          @last_stream_seq = stream_seq
          @next_consumer_seq += 1
          @active = true
          @delivering = false
          @cond.signal unless @callback
        end
      end

      def schedule_recovery(generation)
        start = synchronize do
          return if @closed || !@error.nil? || generation != @generation
          return if @recovering
          return if @delivering

          @recovering = true
          @last_stream_seq + 1
        end
        rebuild(start)
      rescue => e
        fail!(e)
      end

      def recover_after_failed_delivery(generation, stream_seq)
        start = synchronize do
          @delivering = false
          @active = false
          return if @closed || !@error.nil? || generation != @generation
          if @recovering
            @recovery_failed_stream_seq = stream_seq
            return
          end

          @recovering = true
          stream_seq
        end
        rebuild(start)
      rescue => e
        fail!(e)
      end

      def rebuild(start)
        old_sub = synchronize { @sub }
        begin
          start_consumer(
            deliver_policy: "by_start_sequence",
            opt_start_seq: start,
            last_stream_seq: start - 1
          )
        rescue
          unsubscribe_ignoring_errors(old_sub)
          raise
        end
        retry_stream_seq = synchronize { @recovery_failed_stream_seq }
        if retry_stream_seq
          unsubscribe_ignoring_errors(old_sub)
          return rebuild(retry_stream_seq)
        end
        synchronize do
          @recovering = false
        end
        unsubscribe_ignoring_errors(old_sub)
      rescue => e
        synchronize do
          @recovering = false
        end
        fail!(e)
      end

      def start_heartbeat
        interval = @config[:idle_heartbeat] * 2
        @hb_thread = Thread.new do
          loop do
            case @hb_queue.pop(timeout: interval)
            when :stop
              break
            else
              active = synchronize do
                current = @active
                @active = false
                current
              end
              schedule_recovery(synchronize { @generation }) unless active
            end
          end
        rescue => e
          fail!(e)
        end
      end

      def stop_heartbeat
        @hb_queue << :stop
        @hb_thread&.join unless @hb_thread&.equal?(Thread.current)
      end

      def unsubscribe_ignoring_errors(sub)
        sub&.unsubscribe
      rescue
        nil
      end

      def fail!(error)
        sub = synchronize do
          return if @closed || !@error.nil?

          @error = error
          @closed = true
          @recovering = false
          @recovery_failed_stream_seq = nil
          @delivering = false
          @active = false
          @cond.broadcast
          @sub
        end
        begin
          sub&.unsubscribe
        rescue
          nil
        end
        begin
          stop_heartbeat
        rescue
          nil
        end
        @nc.send(:err_cb_call, @nc, error, self)
      end
    end
  end
end
