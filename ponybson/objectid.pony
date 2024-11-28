use "time"
use "buffered"
use "random"
use "format"
use "pony_test"

class ObjectIDGenerator
  let _rand: Rand iso = Rand(Time.seconds().u64())
  let _fuzz: Array[U8] val = [
    _rand.u8()
    _rand.u8()
    _rand.u8()
    _rand.u8()
    _rand.u8()
  ]
  var _counter: U32 = _rand.u32()

  new ref create() => None
  
  fun object_id_from_hex(str: String): OrBsonError[ObjectID val] =>
    try
      recover val ObjectID._create_from_hex(str)? end
    else
      return recover val IllegalObjectIDHexError end
    end
  
  fun object_id_from_bytes(data': Array[U8] val): OrBsonError[ObjectID val] =>
    try
      recover val ObjectID._create_from_bytes(data')? end
    else
      return recover val IllegalObjectIDError end
    end
  
  fun ref new_object_id(): ObjectID val =>
    (let fuzz, let counter) = (_fuzz, _counter = _counter + 1)
    recover val ObjectID._create_from_timestamp(Time.seconds().u32(), fuzz, counter) end

  fun ref object_id_from_timestamp(timestamp': U32): ObjectID val =>
    (let fuzz, let counter) = (_fuzz, _counter = _counter + 1)
    recover val ObjectID._create_from_timestamp(timestamp', fuzz, counter) end
  
  fun empty_object_id(): ObjectID val =>
    recover val ObjectID end

class ObjectID is Stringable
  embed _data: Array[U8] ref = Array[U8](12)

  new val create() =>
    _data.append(Array[U8].init(0, 12))

  new _create_from_timestamp(timestamp': U32, fuzz: Array[U8] val, counter: U32) =>
    var buf = Writer
    buf
      .>reserve_current(12)
      .>u32_be(timestamp')
      .>write(fuzz)
      .>u8((counter >> 16).u8_unsafe())
      .>u8((counter >> 8).u8_unsafe())
      .>u8(counter.u8_unsafe())

    var data_count: USize = 0
    for byteseq in buf.done().values() do
      _data.append(byteseq, data_count = data_count + byteseq.size())
    end
  
  new _create_from_hex(str: String) ? =>
    if str.size() != 24 then
      error
    end
    var index: USize = 0
    while index != 24 do
      _data.push_u8(str.trim(index = index + 2, index).u8(16)?)
    end
  
  new _create_from_bytes(data': Array[U8] val) ? =>
    if (data').size() != 12 then
      error
    end
    _data.append(data')
  
  fun ne(that: box->ObjectID): Bool => not eq(that)
  fun eq(that: box->ObjectID): Bool =>
    var count: USize = 0
    while (count = count + 1) < 12 do
      try
        if _data(count - 1)? != that._data(count - 1)? then return false end
      else
        return false
      end
    end
    true
  
  fun bytes(): Array[U8] val =>
    let tmp: Array[U8] iso = Array[U8](12)
    for byte in _data.values() do
      tmp.push_u8(byte)
    end
    consume val tmp

  fun timestamp(): I64 =>
    try
      let rb = Reader
      let tmp: Array[U8] iso = Array[U8](4)
      for byte in _data.slice(0, 4).values() do
        tmp.push_u8(byte)
      end
      rb.append(consume tmp)
      rb.u32_be()?.i64()
    else
      0
    end
  
  fun hex(): String iso^ =>
    var s: String ref = String
    for byte in _data.values() do
      s.append(Format.int[U8](byte where fmt = FormatHexSmallBare, fill = '0', width = 2))
    end
    s.clone()

  fun string(): String iso^ =>
    var s: String ref = String
    s.append("ObjectID(\"")
    s.append(hex())
    s.append("\")")
    s.clone()

actor ObjectIDTests is TestList
  new make() => None

  fun tag tests(test: PonyTest) =>
    test(_TestObjectID)
    test(_TestUniqueObjectIDFromTimestampFromSameGenerator)
    test(_TestUniqueNewObjectIDFromSameGenerator)
    test(_TestEqualObjectIDFromHex)
    test(_TestEqualRoundtripObjectID)

class iso _TestObjectID is UnitTest
  fun name(): String => "Test Object ID"
  fun apply(h: TestHelper) =>
    h.assert_eq[ObjectID val](ObjectID, ObjectID)

class iso _TestUniqueObjectIDFromTimestampFromSameGenerator is UnitTest
  fun name(): String => "Test Unique ObjectID From Timestamp From Same Generator"
  fun apply(h: TestHelper) =>
    let generator = ObjectIDGenerator
    let timestamp = Time.seconds().u32()
    h.assert_ne[ObjectID val](generator.object_id_from_timestamp(timestamp), generator.object_id_from_timestamp(timestamp))

class iso _TestUniqueNewObjectIDFromSameGenerator is UnitTest
  fun name(): String => "Test Unique New ObjectID From Same Generator"
  fun apply(h: TestHelper) =>
    let generator = ObjectIDGenerator
    h.assert_ne[ObjectID val](generator.new_object_id(), generator.new_object_id())

class iso _TestEqualObjectIDFromHex is UnitTest
  fun name(): String => "Test Equal ObjectID From Hex"
  fun apply(h: TestHelper) =>
    let generator = ObjectIDGenerator
    let oid1 = generator.object_id_from_hex("67480b3c79b4ccf1e973e235")
    let oid2 = generator.object_id_from_hex("67480b3c79b4ccf1e973e235")

    match (oid1, oid2)
    | (let oid1Err: BsonError val, _) => h.fail("Error parsing oid hex: " + oid1Err.string())
    | (_, let oid2Err: BsonError val) => h.fail("Error parsing oid hex: " + oid2Err.string())
    | (let oid1Val: ObjectID val, let oid2Val: ObjectID val) => h.assert_eq[ObjectID val](oid1Val, oid2Val)
    end

class iso _TestEqualRoundtripObjectID is UnitTest
  fun name(): String => "Test Equal Round trip ObjectID"
  fun apply(h: TestHelper) =>
    let generator = ObjectIDGenerator
    let oid1 = generator.new_object_id()
    let oid2 = generator.object_id_from_hex(oid1.hex())

    match oid2
    | (let oid2Err: BsonError val) => h.fail("Error parsing oid hex: " + oid2Err.string())
    | (let oid2Val: ObjectID val) => h.assert_eq[ObjectID val](oid1, oid2Val)
    end
