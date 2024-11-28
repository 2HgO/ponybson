use "collections"
use "time"
use "buffered"
use "format"
use "pony_test"

primitive BsonUtils
  fun _get_kind(reader: Reader, stream_index: USize): OrBsonError[BsonType] =>
    try
      let byte = reader.u8()?
      match byte
      | BsonTypeUnknown() =>
        if reader.size() != 0 then return recover val IllegalBsonTypeError.create(BsonTypeUnknown, stream_index) end end
        BsonTypeUnknown
      | BsonTypeDouble() => BsonTypeDouble
      | BsonTypeStringUTF8() => BsonTypeStringUTF8
      | BsonTypeDocument() => BsonTypeDocument
      | BsonTypeArray() => BsonTypeArray
      | BsonTypeBinary() => BsonTypeBinary
      | BsonTypeUndefined() => BsonTypeUndefined
      | BsonTypeOid() => BsonTypeOid
      | BsonTypeBool() => BsonTypeBool
      | BsonTypeTimeUTC() => BsonTypeTimeUTC
      | BsonTypeNull() => BsonTypeNull
      | BsonTypeRegexp() => BsonTypeRegexp
      | BsonTypeDBPointer() => BsonTypeDBPointer
      | BsonTypeJSCode() => BsonTypeJSCode
      | BsonTypeDeprecated() => BsonTypeDeprecated
      | BsonTypeJSCodeWithScope() => BsonTypeJSCodeWithScope
      | BsonTypeInt32() => BsonTypeInt32
      | BsonTypeTimestamp() => BsonTypeTimestamp
      | BsonTypeInt64() => BsonTypeInt64
      | BsonTypeMaximumKey() => BsonTypeMaximumKey
      | BsonTypeMinimumKey() => BsonTypeMinimumKey
      else
        return recover val UnknownBsonTypeError.create(byte, stream_index) end
      end
    else
      return recover val GenericBsonError.create("illegal hex string at index: " + Format.int[USize](stream_index)) end
    end
  fun _get_bin_kind(reader: Reader, stream_index: USize): OrBsonError[BsonSubType] =>
    try
      let byte = reader.u8()?
      match byte
      | BsonSubTypeGeneric() => BsonSubTypeGeneric
      | BsonSubTypeFunction() => BsonSubTypeFunction
      | BsonSubTypeBinaryOld() => BsonSubTypeBinaryOld
      | BsonSubTypeUuidOld() => BsonSubTypeUuidOld
      | BsonSubTypeUuid() => BsonSubTypeUuid
      | BsonSubTypeMd5() => BsonSubTypeMd5
      | BsonSubTypeUserDefined() => BsonSubTypeUserDefined
      else
        return recover val UnknownBsonTypeError.create(byte, stream_index) end
      end
    else
      return recover val GenericBsonError.create("illegal hex string at index: " + Format.int[USize](stream_index))  end
    end
  fun _get_value(reader: Reader, kind: BsonType, stream_index: USize): OrBsonError[Bson val] =>
    var idx = stream_index
    try
      match kind
      | BsonTypeDouble => recover val Bson.create(kind, reader.f64_le()?) end
      | BsonTypeInt32 => recover val Bson.create(kind, reader.i32_le()?) end
      | BsonTypeInt64 => recover val Bson.create(kind, reader.i64_le()?) end
      | BsonTypeBool => recover val Bson.create(kind, reader.u8()? == 1) end
      | BsonTypeBinary =>
        let val_len = reader.i32_le()?
        idx = idx + I32(0).bytewidth()
        let bin_kind = _get_bin_kind(reader, idx)
        idx = idx + 1
        match bin_kind
        | let err: BsonError val => return err
        | BsonSubTypeMd5 =>
          if val_len != 16 then return recover val GenericBsonError.create("Illegal md5 digest at index: " + Format.int[USize](idx)) end end
        end
        recover val Bson.create(kind, recover val BsonBinaryData.create(bin_kind as BsonSubType, String.from_iso_array(reader.block(val_len.usize())?)) end) end
      | BsonTypeDBPointer =>
        let str_len = reader.i32_le()?
        idx = idx + I32(0).bytewidth()
        let ref_col = String.from_iso_array(reader.block(str_len.usize() - 1)?)
        reader.u8()? // -- discard null byte
        idx = idx + str_len.usize()
        if reader.size() < 12 then return recover val GenericBsonError.create("Illegal object id at index: " + Format.int[USize](idx)) end end
        let ref_oid = recover val ObjectID._create_from_bytes(reader.block(12)?)? end
        idx = idx + 12
        recover val Bson.create(kind, recover val BsonDBPointerData.create(consume ref_col, ref_oid) end) end
      | BsonTypeRegexp =>
        let regex = String.from_iso_array(reader.read_until(0x00)?)
        idx = idx + regex.size() + 1
        let options = String.from_iso_array(reader.read_until(0x00)?)
        idx = idx + options.size() + 1
        recover val Bson.create(kind, recover val BsonRegexpData.create(consume regex, consume options) end) end
      | BsonTypeJSCode =>
        let str_len = reader.i32_le()?
        idx = idx + I32(0).bytewidth()
        if reader.size() < str_len.usize() then return recover val GenericBsonError.create("Truncated string at index: " + Format.int[USize](idx) + " expected string of length: " + Format.int[I32](str_len)) end end
        let str_val = String.from_iso_array(reader.block(str_len.usize() - 1)?)
        reader.u8()? // -- discard null byte
        idx = idx + str_len.usize()
        recover val Bson.create(kind, consume val str_val) end
      | BsonTypeTimestamp => recover val Bson.create(kind, recover val BsonTimestampData.create(reader.i32_le()?, reader.i32_le()?) end) end
      | BsonTypeTimeUTC =>
        let datetime = reader.i64_le()?.f64()
        idx = idx + I64(0).bytewidth()
        recover val Bson.create(kind, recover val PosixDate.create((datetime / 1000).i64(), ((datetime %% 1000) * 1000000).i64()) end) end
      | BsonTypeOid =>
        if reader.size() < 12 then return recover val GenericBsonError.create("Illegal object id at index: " + Format.int[USize](idx)) end end
        recover val Bson.create(kind, recover val ObjectID._create_from_bytes(reader.block(12)?)? end) end
      | BsonTypeStringUTF8 =>
        let str_len = reader.i32_le()?
        idx = idx + I32(0).bytewidth()
        if reader.size() < str_len.usize() then return recover val GenericBsonError.create("Truncated string at index: " + Format.int[USize](idx) + " expected string of length: " + Format.int[I32](str_len)) end end
        let str_val = String.from_iso_array(reader.block(str_len.usize() - 1)?)
        reader.u8()? // -- discard null byte
        idx = idx + str_len.usize()
        recover val Bson.create(kind, consume val str_val) end
      | BsonTypeArray =>
        let data_len = reader.i32_le()?
        idx = idx + I32(0).bytewidth()
        let root: Array[Bson val] iso = recover Array[Bson val] end
        let new_reader: Reader = Reader
        new_reader.append(reader.block(data_len.usize() - I32(0).bytewidth())?)
        while new_reader.size() > 0 do
          let val_kind = _get_kind(new_reader, idx)
          idx = idx + 1
          match val_kind
          | let err: BsonError val => return err
          | BsonTypeUnknown => continue
          end
          let val_idx = String.from_iso_array(new_reader.read_until(0x00)?) // -- discard index
          idx = idx + val_idx.size() + 1
          let off = new_reader.size()
          let val_data = _get_value(new_reader, val_kind as BsonType, idx)
          match val_data
          | let err: BsonError val => return err
          end
          root.push(val_data as Bson val)
          idx = idx + (off - new_reader.size())
        end
        recover val Bson.create(kind, consume val root) end
      | BsonTypeDocument =>
        let data_len = reader.i32_le()?
        idx = idx + I32(0).bytewidth()
        let root: Map[String val, Bson val] iso = recover Map[String val, Bson val] end
        let new_reader: Reader = Reader
        new_reader.append(reader.block(data_len.usize() - I32(0).bytewidth())?)
        while new_reader.size() > 0 do
          let val_kind = _get_kind(new_reader, idx)
          idx = idx + 1
          match val_kind
          | let err: BsonError val => return err
          | BsonTypeUnknown => continue
          end
          let val_key = String.from_iso_array(new_reader.read_until(0x00)?)
          idx = idx + val_key.size() + 1
          let off = new_reader.size()
          let val_data = _get_value(new_reader, val_kind as BsonType, idx)
          match val_data
          | let err: BsonError val => return err
          end
          root.insert(consume val_key, val_data as Bson val)
          idx = idx + (off - new_reader.size())
        end
        recover val Bson.create(kind, consume val root) end
      else
        recover val Bson.create(kind, None) end
      end
    else
      return recover val GenericBsonError.create("illegal hex string at index: " + Format.int[USize](idx))  end
    end
  fun from_bytes(reader: Reader): OrBsonError[Bson val] =>
    _get_value(reader, BsonTypeDocument, 0)

actor BsonUtilsTests is TestList
  new make() => None

  fun tag tests(test: PonyTest) =>
    test(_TestParseBsonType)
    test(_TestParseBsonDocument)

class iso _TestParseBsonType is UnitTest
  fun name(): String => "Test Parse Bson Type"
  fun apply(h: TestHelper) =>
    let r: Reader = Reader
    r.append([BsonTypeDouble()])
    let kind = BsonUtils._get_kind(r, 0)
    match kind
    | let kErr: BsonError val => h.fail("Error parsing bson type: " + kErr.string())
    | let kVal: BsonType => h.assert_is[BsonType](kVal, BsonTypeDouble)
    end

class iso _TestParseBsonDocument is UnitTest
  var data: (Array[U8] val | None) = None
  var doc: Map[String val, Bson val] val = recover Map[String val, Bson val] end

  fun ref set_up(h: TestHelper)? =>
    let date: PosixDate iso = recover
      let g = PosixDate
      g.year = 2000
      g.month = 1
      g.day_of_month = 1
      g.normal()
      g
    end
    doc = recover
      Map[String val, Bson val]
        .>insert("oid", Bson.create(BsonTypeOid, recover val ObjectID._create_from_hex("67480b3c79b4ccf1e973e235")? end))
        .>insert("int32_max", Bson.create(BsonTypeInt32, I32.max_value()))
        .>insert("int32_min", Bson.create(BsonTypeInt32, I32.min_value()))
        .>insert("int64_max", Bson.create(BsonTypeInt64, I64.max_value()))
        .>insert("int64_min", Bson.create(BsonTypeInt64, I64.min_value()))
        .>insert("float_max", Bson.create(BsonTypeDouble, F64.max_value()))
        // .>insert("float_min", Bson.create(BsonTypeDouble, F64(4.94e-324)))
        .>insert("time_utc", Bson.create(BsonTypeTimeUTC, consume date))
        .>insert("timestamp", Bson.create(BsonTypeTimestamp, recover BsonTimestampData.create(1000, 1732778369) end))
        .>insert("null", Bson.create(BsonTypeNull, None))
        .>insert("undefined", Bson.create(BsonTypeUndefined, None))
        .>insert("string", Bson.create(BsonTypeStringUTF8, "random string"))
    end
    // externally marshalled using [the golang bson lib](https://pkg.go.dev/go.mongodb.org/mongo-driver/bson)
    data = [
      189
      0
      0
      0
      7
      111
      105
      100
      0
      103
      72
      11
      60
      121
      180
      204
      241
      233
      115
      226
      53
      16
      105
      110
      116
      51
      50
      95
      109
      97
      120
      0
      255
      255
      255
      127
      16
      105
      110
      116
      51
      50
      95
      109
      105
      110
      0
      0
      0
      0
      128
      18
      105
      110
      116
      54
      52
      95
      109
      97
      120
      0
      255
      255
      255
      255
      255
      255
      255
      127
      18
      105
      110
      116
      54
      52
      95
      109
      105
      110
      0
      0
      0
      0
      0
      0
      0
      0
      128
      1
      102
      108
      111
      97
      116
      95
      109
      97
      120
      0
      255
      255
      255
      255
      255
      255
      239
      127
      9
      116
      105
      109
      101
      95
      117
      116
      99
      0
      0
      172
      207
      106
      220
      0
      0
      0
      17
      116
      105
      109
      101
      115
      116
      97
      109
      112
      0
      232
      3
      0
      0
      129
      25
      72
      103
      10
      110
      117
      108
      108
      0
      6
      117
      110
      100
      101
      102
      105
      110
      101
      100
      0
      2
      115
      116
      114
      105
      110
      103
      0
      14
      0
      0
      0
      114
      97
      110
      100
      111
      109
      32
      115
      116
      114
      105
      110
      103
      0
      0
    ]
  fun name(): String => "Test Parse Bson Document"
  fun apply(h: TestHelper)? =>
    let r: Reader = Reader
    r.append(data as Array[U8] val)
    h.assert_array_eq_unordered[U8](data as Array[U8] val, Bson.create(BsonTypeDocument, doc).bytes())
    let kind = BsonUtils.from_bytes(r)
    match kind
    | let kErr: BsonError val => h.fail("Error parsing bson: " + kErr.string())
    | let kVal: Bson val =>
      h.assert_is[BsonType](kVal.kind, BsonTypeDocument)
      for (k, v) in (kVal.data as Map[String val, Bson val] val).pairs() do
        h.assert_eq[Bson val](v, doc(k)?)
      end
    end



