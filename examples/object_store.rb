# frozen_string_literal: true

# Two-chunk Object Store upload and download, streaming one chunk at a time.
#
# With a live NATS server this is:
#
#   nc = NATS.connect("nats://127.0.0.1:4222")
#   js = nc.jetstream
#   store = js.create_object_store("FILES")
#
# The example runs offline against the fake JetStream layer from the specs,
# so no server is needed.

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
$LOAD_PATH.unshift(File.expand_path("../spec", __dir__))

require "nats/io/jetstream"
require "stringio"
require "support/fake_object_store_jetstream"

backend = FakeObjectStoreJetStreamBackend.new
js = FakeObjectStoreJetStream.new(backend)
store = js.create_object_store("FILES")

# 3 KiB object with a 2 KiB chunk size => exactly two chunks.
object = ("A" * 2048) + ("B" * 1024)
chunk_size = 2048

source = StringIO.new(object)

store.put("two-chunks.bin", source, chunk_size: chunk_size)

info = store.get_info("two-chunks.bin")
puts "object   : #{info.name} in bucket #{info.bucket}"
puts "chunks   : #{info.chunks}"
puts "size     : #{info.size} bytes"
puts "digest   : #{info.digest}"
puts

# Download chunk by chunk. The blocks never accumulate into the whole
# object: peak working memory is one chunk plus the running SHA-256 state,
# regardless of how large the object is.
sink = StringIO.new
received = 0
max_seen = 0
store.get("two-chunks.bin") do |chunk, chunk_info|
  max_seen = [max_seen, chunk.bytesize].max
  sink.write(chunk)
  received += chunk.bytesize
  puts "chunk    : #{chunk.bytesize} bytes (running total #{received}/#{chunk_info.size})"
end

puts
puts "roundtrip ok : #{sink.string == object}"
puts "peak chunk   : #{max_seen} bytes"
puts "object size  : #{object.bytesize} bytes"
puts
puts "Growing the object (more chunks) leaves the peak chunk at #{chunk_size} bytes;"
puts "only changing chunk_size grows per-chunk memory."

# Show the property explicitly with a larger object.
big = StringIO.new("Z" * (chunk_size * 8))
store.put("eight-chunks.bin", big, chunk_size: chunk_size)
peak = 0
store.get("eight-chunks.bin") { |chunk, _| peak = [peak, chunk.bytesize].max }
puts "8 chunks, peak held per chunk: #{peak} bytes"
