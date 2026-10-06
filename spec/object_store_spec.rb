# frozen_string_literal: true

require "stringio"

describe "ObjectStore" do
  # The fake JetStream layer lives in spec/support.
  let(:backend) { FakeObjectStoreJetStreamBackend.new }
  let(:js) { FakeObjectStoreJetStream.new(backend) }

  it "creates an OBJ stream with the Object Store configuration" do
    js.create_object_store(bucket: "FILES", description: "d", max_bytes: 1024)

    stream = backend.stream_info("OBJ_FILES")
    expect(stream[:config][:subjects]).to eql(["$O.FILES.>"])
    expect(stream[:config][:allow_rollup_hdrs]).to eql(true)

    expect { js.object_store("MISSING") }.to raise_error(NATS::ObjectStore::BucketNotFoundError)
  end

  it "uploads from an IO chunk by chunk and binds chunks, size and digest" do
    store = js.create_object_store("FILES")
    payload = "X" * 2050
    chunk_size = 1024
    chunks = []

    info = store.put("dir/a.txt", StringIO.new(payload), chunk_size: chunk_size,
      description: "a file", headers: { "Color" => "blue" })

    expect(info.chunks).to eql(3)
    expect(info.size).to eql(2050)
    expected = "SHA-256=" + Base64.urlsafe_encode64(Digest::SHA256.digest(payload), padding: true)
    expect(info.digest).to eql(expected)

    chunk_msgs = backend.published.select { |msg| msg.subject.start_with?("$O.FILES.C.") }
    expect(chunk_msgs.map { |msg| msg.data.bytesize }).to eql([1024, 1024, 2])
    expect(chunk_msgs.map { |msg| msg.headers }).to all(be_nil)
    chunk_subjects = chunk_msgs.map(&:subject).uniq
    expect(chunk_subjects.size).to eql(1)

    meta = backend.published.find { |msg| msg.subject.start_with?("$O.FILES.M.") }
    expect(meta.subject).to eql(chunk_subjects[0].sub("C.", "M."))
    expect(meta.headers[NATS::ObjectStore::HDR_CHUNK_SIZE]).to eql("1024")
    expect(meta.headers[NATS::ObjectStore::HDR_DELETE]).to be_nil

    wire = JSON.parse(meta.data)
    expect(wire.fetch_values("name", "bucket", "chunks", "size", "digest", "deleted")).to(
      eql(["dir/a.txt", "FILES", 3, 2050, expected, false])
    )
    expect(wire["options"]["desc"]).to eql("a file")
    expect(Base64.strict_decode64(wire["options"]["headers"])).to include("Color: blue")

    store.get("dir/a.txt") { |chunk, _info| chunks << chunk }
    expect(chunks.join.bytesize).to eql(2050)
    expect(chunks.join).to eql(payload)

    # Chunks alone are not a complete object: the latest metadata message
    # is what makes the object visible.
    expect { store.get_info("dir/absent.txt") }.to raise_error(NATS::ObjectStore::ObjectNotFoundError)
  end

  it "downloads by block and by enumerator, and verifies size and digest" do
    store = js.create_object_store("FILES")
    payload = "chunk-streaming" * 100
    store.put("blob", StringIO.new(payload), chunk_size: 64)

    enumerator = store.get("blob")
    expect(enumerator).to be_an(Enumerator)
    expect(enumerator.to_a.map(&:first).join).to eql(payload)

    # Tamper with one chunk and the digest check must fail.
    stream = backend.stream_info("OBJ_FILES")
    target = stream[:messages].find { |msg| msg.subject.start_with?("$O.FILES.C.") }
    target.data = target.data.dup
    target.data.setbyte(0, (target.data.getbyte(0) + 1) % 256)
    expect do
      store.get("blob") { |_chunk, _info| }
    end.to raise_error(NATS::ObjectStore::InvalidChainError, /digest mismatch/)
  end

  it "reports a missing chunk and a metadata size mismatch" do
    store = js.create_object_store("FILES")
    store.put("full", StringIO.new("abcdefghi"), chunk_size: 3)

    # Drop the last chunk, as if the stream lost part of the object.
    stream = backend.stream_info("OBJ_FILES")
    last_chunk = stream[:messages].select { |msg| msg.subject.start_with?("$O.FILES.C.") }.last
    stream[:messages].delete(last_chunk)
    expect do
      store.get("full") { |_chunk, _info| }
    end.to raise_error(NATS::ObjectStore::InvalidChainError, /expected 3 chunks/)

    # Rewrite the size in the metadata to mismatch the chunks.
    store.put("sized", StringIO.new("xy"), chunk_size: 2)
    meta = stream[:messages].find do |msg|
      msg.subject.start_with?("$O.FILES.M.") && JSON.parse(msg.data)["name"] == "sized"
    end
    wire = JSON.parse(meta.data)
    wire["size"] = 999
    meta.data = wire.to_json
    expect do
      store.get("sized") { |_chunk, _info| }
    end.to raise_error(NATS::ObjectStore::InvalidChainError, /size mismatch/)
  end

  it "pins an in-flight download to one version across an overwrite" do
    store = js.create_object_store("FILES")
    store.put("v", StringIO.new("one"), chunk_size: 3)
    first = store.get_info("v")

    # Start reading the old version after the overwrite is already visible.
    store.put("v", StringIO.new("two!!"), chunk_size: 4)
    chunks = []
    # Re-resolving would return the new version; the recorded NUID selects
    # the chunks of the old one through its per-version chunk subject.
    store.send(:stream_chunks, first) { |chunk, _info| chunks << chunk }
    expect(chunks.join).to eql("one")

    expect(store.get_info("v").nuid).not_to eql(first.nuid)
  end

  it "deletes an object with a tombstone metadata message and rolls its chunks up" do
    store = js.create_object_store("FILES")
    store.put("gone", StringIO.new("bye"), chunk_size: 3)
    info = store.get_info("gone")

    store.delete("gone")

    stream = backend.stream_info("OBJ_FILES")
    chunk_subject = "$O.FILES.C.#{info.nuid}"
    expect(stream[:messages].map(&:subject)).not_to include(chunk_subject)

    tombstone = stream[:messages].select { |msg| msg.subject.start_with?("$O.FILES.M.") }.last
    expect(tombstone.headers[NATS::ObjectStore::HDR_DELETE]).to eql("true")
    expect(JSON.parse(tombstone.data)["deleted"]).to eql(true)

    expect { store.get_info("gone") }.to raise_error(NATS::ObjectStore::ObjectDeletedError)
    expect { store.delete("gone") }.to raise_error(NATS::ObjectStore::ObjectDeletedError)
  end

  it "cleans the temporary chunks of a failed upload and leaves no object" do
    store = js.create_object_store("FILES")
    failing = Object.new
    def failing.read(_size)
      @reads = (@reads || 0) + 1
      raise "disk error" if @reads == 2

      "abc"
    end

    expect { store.put("broken", failing, chunk_size: 3) }.to raise_error(RuntimeError, "disk error")

    stream = backend.stream_info("OBJ_FILES")
    expect(stream[:messages].select { |msg| msg.subject.start_with?("$O.FILES.C.") }).to be_empty
    expect { store.get_info("broken") }.to raise_error(NATS::ObjectStore::ObjectNotFoundError)

    # The cleanup uses a subject rollup, the documented server primitive.
    rollup = backend.published.find { |msg| msg.headers && msg.headers[NATS::ObjectStore::ROLLUP] }
    expect(rollup.headers[NATS::ObjectStore::ROLLUP]).to eql(NATS::ObjectStore::MSG_ROLLUP_SUBJECT)
    expect(rollup.subject).to start_with("$O.FILES.C.")
  end

  it "creates same-bucket links, cross-bucket links and rejects cycles" do
    files = js.create_object_store("FILES")
    other = js.create_object_store("OTHER")
    files.put("target", StringIO.new("payload"), chunk_size: 7)
    other.put("target", StringIO.new("remote"), chunk_size: 6)

    files.add_link("alias", "target")
    expect(files.get_info("alias").name).to eql("target")
    downloaded = +""
    files.get("alias") { |chunk, _| downloaded << chunk }
    expect(downloaded).to eql("payload")

    # A cross-bucket link resolves through the shared JetStream context.
    files.add_link("remote-link", "target", target_bucket: "OTHER")
    downloaded = +""
    files.get("remote-link") { |chunk, _| downloaded << chunk }
    expect(downloaded).to eql("remote")

    # A link that points at itself is a cycle.
    files.add_link("selfish", "selfish")
    expect { files.get_info("selfish") }.to raise_error(NATS::ObjectStore::LinkCycleError)

    # A -> B -> A is also a cycle.
    files.add_link("a", "b")
    files.add_link("b", "a")
    expect { files.get_info("a") }.to raise_error(NATS::ObjectStore::LinkCycleError)

    # A whole-bucket link has no downloadable object behind it.
    files.add_bucket_link("bucket-link", "OTHER")
    expect { files.get_info("bucket-link") }.to raise_error(NATS::ObjectStore::InvalidLinkError)
  end

  it "limits the depth of link chains" do
    store = js.create_object_store("FILES")
    store.put("leaf", StringIO.new("x"), chunk_size: 1)
    (1..(NATS::ObjectStore::MAX_LINK_DEPTH + 2)).each_cons(2).to_a.each do |from, to|
      store.add_link("link#{from}", "link#{to}")
    end
    store.add_link("link#{NATS::ObjectStore::MAX_LINK_DEPTH + 2}", "leaf")

    expect { store.get_info("link1") }.to raise_error(NATS::ObjectStore::TooManyLinksError)
  end

  it "stores empty objects with zero chunks and a digest of the empty input" do
    store = js.create_object_store("FILES")
    info = store.put("empty", StringIO.new(""), chunk_size: 4)

    expect(info.chunks).to eql(0)
    expect(info.size).to eql(0)
    expect(info.digest).to eql(NATS::ObjectStore.digest_of(""))
    result = store.get("empty") { |_chunk, _info| }
    expect(result.name).to eql(info.name)
    expect(result.digest).to eql(info.digest)
  end

  it "rejects invalid names and missing link targets" do
    store = js.create_object_store("FILES")
    expect { store.put(".hidden", StringIO.new("x")) }.to raise_error(NATS::ObjectStore::InvalidObjectNameError)
    expect { store.add_link("link", ".missing") }.to raise_error(NATS::ObjectStore::InvalidObjectNameError)
    expect { store.get_info("link") }.to raise_error(NATS::ObjectStore::ObjectNotFoundError)
  end
end
