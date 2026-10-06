# frozen_string_literal: true

# Copyright 2026 The NATS Authors
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

require_relative "../errors"

module NATS
  class ObjectStore
    class Error < NATS::Error; end

    # When the bucket does not exist or is not an Object Store bucket.
    class BucketNotFoundError < Error
      def to_s
        "nats: bucket not found"
      end
    end

    class BadBucketError < Error; end

    # When the requested object does not exist.
    class ObjectNotFoundError < Error
      attr_reader :object

      def initialize(object = nil)
        @object = object
      end

      def to_s
        "nats: object not found#{" '#{@object}'" if @object}"
      end
    end

    # When the object existed but was deleted.
    class ObjectDeletedError < ObjectNotFoundError
      def to_s
        "nats: object '#{@object}' was deleted"
      end
    end

    # When the object name or another argument is invalid.
    class InvalidObjectNameError < Error
      def to_s
        "nats: invalid object name"
      end
    end

    class InvalidLinkError < Error; end

    # When a chain of links points back to an object that it already
    # followed.
    class LinkCycleError < Error
      def to_s
        "nats: link cycle detected"
      end
    end

    # When a chain of links is longer than ObjectStore::MAX_LINK_DEPTH.
    class TooManyLinksError < Error
      def to_s
        "nats: maximum link depth of #{ObjectStore::MAX_LINK_DEPTH} exceeded"
      end
    end

    # When the chunks of an object do not match its metadata, for example
    # because a block is missing or its digest differs.
    class InvalidChainError < Error; end
  end
end
