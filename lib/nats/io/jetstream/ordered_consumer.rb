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

require_relative "errors"

module NATS
  class JetStream
    # OrderedConsumer delivers the messages of a stream to a block, one at
    # a time and in their stream order, for analysis tasks that read the
    # stream once. It consumes through an ephemeral push consumer, which
    # it recreates from just after the last delivered stream sequence when
    # it loses deliveries: when the consumer sequence of a delivery is not
    # the expected one, or when an idle heartbeat reports deliveries that
    # never arrived.
    #
    # The consumer sequence counts the deliveries of one consumer, the
    # stream sequence the messages of the stream: a consumer with a filter
    # subject sees the stream sequence jump without losing anything, so
    # only the consumer sequence tells a lost delivery. Every recreation
    # starts a new generation; deliveries and control frames of a replaced
    # generation are dropped, so a late message of an old consumer and a
    # recovery that races another one cannot deliver a message twice or
    # leave two recoveries running.
    #
    # A message counts as delivered when the block returns normally for
    # it. When the block raises, the message does not count as delivered,
    # consumption stops, and #each raises the error. Any error that stops
    # consumption, from the block or from recreating the consumer, ends
    # the wait of #each with that error.
    #
    # @example Read a stream once, in order.
    #   nc = NATS.connect
    #   js = nc.jetstream
    #   ordered = js.ordered_consumer("EVENTS", filter_subject: "events.>")
    #   ordered.each do |msg|
    #     process(msg)
    #   end
    #
    # @!visibility public
    class OrderedConsumer
      include MonitorMixin

      # Default seconds between the idle heartbeats of the consumer.
      DEFAULT_IDLE_HEARTBEAT = 5

      # Default seconds after which the server removes the idle ephemeral consumer.
      DEFAULT_INACTIVE_THRESHOLD = 5 * 60

      # Status of the control frame that says the consumer was deleted.
      CONSUMER_DELETED = "409"

      attr_reader :stream

      def initialize(js, stream, params = {})
        super() # required to initialize the monitor
        raise JetStream::Error::InvalidStreamName.new("nats: invalid stream name") if stream.nil? || stream.empty?

        @js = js
        @nc = js.nc
        @stream = stream
        @filter_subject = params[:filter_subject]
        @deliver_policy = params[:deliver_policy] || "all"
        @opt_start_seq = params[:opt_start_seq]
        @idle_heartbeat = params[:idle_heartbeat] || DEFAULT_IDLE_HEARTBEAT
        @inactive_threshold = params[:inactive_threshold] || DEFAULT_INACTIVE_THRESHOLD

        @callback = nil
        @sub = nil
        @consumer_name = nil
        # Generation of the current consumer, bumped on every (re)creation;
        # deliveries of another generation are dropped.
        @serial = 0
        # Consumer sequence that the current generation delivers next.
        @expected_dseq = 1
        # Stream sequence of the last delivered message; 0 before the first.
        @cursor_sseq = 0
        @delivered = 0
        @stopped = false
        @error = nil
        @cond = new_cond
        # Serializes recovery, so that triggers that race start at most one.
        @reset_mutex = Mutex.new
      end

      # consumer_name is the name of the ephemeral consumer of the current
      # generation.
      # @return [String, nil] nil before the first consumer is created.
      def consumer_name
        synchronize { @consumer_name }
      end

      # delivered is the number of messages delivered to the block so far.
      # @return [Integer]
      def delivered
        synchronize { @delivered }
      end

      # each creates the ephemeral consumer and delivers every message to
      # the block in stream order, recreating the consumer when deliveries
      # are lost, until #stop or an error that cannot be recovered.
      #
      # @yield [NATS::Msg] Each message, in stream order.
      # @raise [NATS::JetStream::Error] When the consumer cannot be created.
      # @raise [Exception] Whatever the block raises; the message it raised
      #   on does not count as delivered.
      # @return [self]
      def each(&block)
        raise JetStream::Error.new("nats: ordered consumer requires a block") unless block

        synchronize do
          raise JetStream::Error.new("nats: ordered consumer already started") if @callback

          @callback = block
        end
        reset_consumer(nil)
        synchronize do
          @cond.wait_until { @stopped }
          raise @error if @error
        end
        self
      end

      # stop ends the wait of #each and unsubscribes from the consumer.
      # @return [void]
      def stop
        sub = synchronize do
          @stopped = true
          @cond.broadcast
          @sub
        end
        sub&.unsubscribe
        nil
      end

      private

      # process handles a message delivered to the subscription of one
      # generation of the consumer; serial tells which.
      def process(msg, serial)
        if JS.is_status_msg(msg)
          process_status(msg, serial)
          return
        end
        return if synchronize { @stopped || serial != @serial }

        meta = msg.metadata
        raise JetStream::Error::NotJSMessage.new("nats: not a jetstream message") unless meta

        action = synchronize do
          if @stopped || serial != @serial
            # A late message of a consumer generation that was replaced.
            :drop
          elsif meta.sequence.stream <= @cursor_sseq
            # Already delivered: the delivered stream sequences only increase.
            :drop
          elsif meta.sequence.consumer == @expected_dseq
            @expected_dseq += 1
            :deliver
          else
            # The consumer sequence skipped: deliveries were lost.
            :reset
          end
        end
        case action
        when :deliver then deliver(msg)
        when :reset then recover(serial)
        end
      rescue JetStream::Error::NotJSMessage => e
        fail_with(e)
      end

      # process_status handles a control frame: a flow control request, an
      # idle heartbeat, or a status that ends the consumer.
      def process_status(msg, serial)
        status = msg.header[JS::Header::Status]
        desc = msg.header[JS::Header::Desc].to_s
        case status
        when JS::Status::CtrlMsg
          if desc.start_with?("FlowControl")
            # Flow control asks the client to confirm it is still there;
            # the reply goes to the consumer, stale generation or not.
            @nc.publish(msg.reply) if msg.reply && !msg.reply.empty?
          elsif desc.start_with?("Idle")
            # An idle heartbeat reports the last delivery of the consumer;
            # deliveries past the expected one were lost. A heartbeat of a
            # generation being replaced says nothing about the new one.
            last_consumer = msg.header[JS::Header::LastConsumerSeq].to_i
            missed = synchronize { !@stopped && serial == @serial && last_consumer >= @expected_dseq }
            recover(serial) if missed
          end
          # Other control frames do not concern the delivery order.
        when CONSUMER_DELETED
          # The consumer is gone, as after a server restart; recreate it.
          recover(serial)
        else
          error = begin
            JS.from_msg(msg)
          rescue JetStream::Error => e
            e
          end
          fail_with(error)
        end
      end

      # deliver passes a message to the block, for which it counts as
      # delivered when the block returns normally.
      def deliver(msg)
        @callback.call(msg)
        synchronize do
          @cursor_sseq = msg.metadata.sequence.stream
          @delivered += 1
        end
      rescue => e
        fail_with(e)
      end

      # recover recreates the consumer after lost deliveries, unless a
      # recovery that another trigger started already replaced it.
      def recover(serial)
        reset_consumer(serial)
      rescue => e
        fail_with(e)
      end

      # reset_consumer replaces the ephemeral consumer with one of a new
      # generation that delivers from just after the last delivered
      # message. A trigger passes the serial of the generation in which it
      # saw the loss; when that is no longer the current generation, a
      # recovery already replaced the consumer and this one does nothing.
      def reset_consumer(serial)
        @reset_mutex.synchronize do
          generation = synchronize do
            return if @stopped || (serial && serial != @serial)

            @serial += 1
            @expected_dseq = 1
            @serial
          end

          deliver_subject = @nc.new_inbox
          new_sub = @nc.subscribe(deliver_subject) { |msg| process(msg, generation) }
          config = API::ConsumerConfig.new(consumer_config)
          config.deliver_subject = deliver_subject
          cinfo = @js.add_consumer(@stream, config)

          old_sub, stopped = synchronize do
            if @stopped
              [nil, true]
            else
              old = @sub
              @sub = new_sub
              @consumer_name = cinfo.name
              [old, false]
            end
          end
          if stopped
            new_sub.unsubscribe
          else
            # Late messages of the old subscription carry its generation,
            # so unsubscribing it now cannot lose anything new.
            old_sub&.unsubscribe
          end
        end
      end

      # consumer_config is the config of the next ephemeral consumer:
      # ordered delivery asks for no acks, and for flow control with
      # heartbeats to notice lost deliveries, from a consumer stored in
      # memory on a single replica.
      def consumer_config
        config = {
          ack_policy: JS::Config::AckNone,
          max_deliver: 1,
          ack_wait: 22 * 3600,
          flow_control: true,
          idle_heartbeat: @idle_heartbeat,
          inactive_threshold: @inactive_threshold,
          num_replicas: 1,
          mem_storage: true
        }
        config[:filter_subject] = @filter_subject if @filter_subject
        cursor = synchronize { @cursor_sseq }
        if cursor > 0
          # Recover from just after the last delivered message.
          config[:deliver_policy] = "by_start_sequence"
          config[:opt_start_seq] = cursor + 1
        else
          config[:deliver_policy] = @deliver_policy
          config[:opt_start_seq] = @opt_start_seq if @opt_start_seq
        end
        config
      end

      # fail_with stops consumption with an error that #each then raises.
      def fail_with(error)
        sub = synchronize do
          return if @stopped

          @error = error
          @stopped = true
          @cond.broadcast
          @sub
        end
        sub&.unsubscribe
      end
    end
  end
end
