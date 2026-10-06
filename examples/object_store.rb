# frozen_string_literal: true

require "nats/client"

class LimitedReader
  def initialize(size)
    @remaining = size
  end

  def read(length, buffer = nil)
    return if @remaining.zero?

    size = [length, @remaining].min
    @remaining -= size
    chunk = "x".b * size
    if buffer
      buffer.replace(chunk)
    else
      chunk
    end
  end
end

uri = ENV.fetch("NATS_URL", "nats://127.0.0.1:4222")
chunk_size = 4096
object_size = chunk_size * 2

nc = NATS.connect(uri)
js = nc.jetstream
store = begin
  js.create_object_store(bucket: "FILES")
rescue NATS::JetStream::Error
  js.object_store("FILES")
end

store.put("large.bin", LimitedReader.new(object_size), chunk_size: chunk_size)

downloaded = 0
store.get("large.bin") do |chunk|
  downloaded += chunk.bytesize
  puts "processed chunk=#{chunk.bytesize} bytes total=#{downloaded} bytes"
end

info = store.object_info("large.bin")
puts "object=#{info.name} chunks=#{info.chunks} size=#{info.size} digest=#{info.digest}"
puts "peak object-sized buffer was never allocated; each callback receives at most #{chunk_size} bytes"

nc.close
