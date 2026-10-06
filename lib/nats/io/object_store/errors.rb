# frozen_string_literal: true

require_relative "../errors"

module NATS
  class ObjectStore
    class Error < NATS::Error; end

    class InvalidObjectNameError < Error
      def to_s
        "nats: invalid object name"
      end
    end

    class InvalidObjectStoreNameError < Error
      def to_s
        "nats: invalid object store bucket name"
      end
    end

    class InvalidObjectOptionsError < Error
      def to_s
        "nats: invalid object options"
      end
    end

    class BucketNotFoundError < Error
      def to_s
        "nats: object store bucket not found"
      end
    end

    class BadBucketError < Error
      def to_s
        "nats: invalid object store bucket"
      end
    end

    class ObjectNotFoundError < Error
      def to_s
        "nats: object not found"
      end
    end

    class ObjectDeletedError < Error
      def to_s
        "nats: object was deleted"
      end
    end

    class InvalidObjectError < Error; end

    class DigestMismatchError < Error
      def to_s
        "nats: object digest mismatch"
      end
    end

    class SizeMismatchError < Error
      def to_s
        "nats: object size mismatch"
      end
    end

    class ChunkMismatchError < Error
      def to_s
        "nats: object chunk mismatch"
      end
    end

    class InvalidLinkError < Error; end

    class LinkNotFoundError < Error
      def to_s
        "nats: object link not found"
      end
    end

    class LinkCycleError < Error
      def to_s
        "nats: object link cycle"
      end
    end

    class LinkDepthExceededError < Error
      def to_s
        "nats: object link depth exceeded"
      end
    end
  end
end
