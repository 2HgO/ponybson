use "collections"
use "time"
use "buffered"
use "format"

// -- Bson types
primitive BsonTypeUnknown
  fun apply(): U8 => 0x00
  fun string(): String iso^ => "BsonTypeUnknown".clone()
primitive BsonTypeDouble
  fun apply(): U8 => 0x01
  fun string(): String iso^ => "BsonTypeDouble".clone()
primitive BsonTypeStringUTF8
  fun apply(): U8 => 0x02
  fun string(): String iso^ => "BsonTypeStringUTF8".clone()
primitive BsonTypeDocument
  fun apply(): U8 => 0x03
  fun string(): String iso^ => "BsonTypeDocument".clone()
primitive BsonTypeArray
  fun apply(): U8 => 0x04
  fun string(): String iso^ => "BsonTypeArray".clone()
primitive BsonTypeBinary
  fun apply(): U8 => 0x05
  fun string(): String iso^ => "BsonTypeBinary".clone()
primitive BsonTypeUndefined
  fun apply(): U8 => 0x06
  fun string(): String iso^ => "BsonTypeUndefined".clone()
primitive BsonTypeOid
  fun apply(): U8 => 0x07
  fun string(): String iso^ => "BsonTypeOid".clone()
primitive BsonTypeBool
  fun apply(): U8 => 0x08
  fun string(): String iso^ => "BsonTypeBool".clone()
primitive BsonTypeTimeUTC
  fun apply(): U8 => 0x09
  fun string(): String iso^ => "BsonTypeTimeUTC".clone()
primitive BsonTypeNull
  fun apply(): U8 => 0x0A
  fun string(): String iso^ => "BsonTypeNull".clone()
primitive BsonTypeRegexp
  fun apply(): U8 => 0x0B
  fun string(): String iso^ => "BsonTypeRegexp".clone()
primitive BsonTypeDBPointer
  fun apply(): U8 => 0x0C
  fun string(): String iso^ => "BsonTypeDBPointer".clone()
primitive BsonTypeJSCode
  fun apply(): U8 => 0x0D
  fun string(): String iso^ => "BsonTypeJSCode".clone()
primitive BsonTypeDeprecated
  fun apply(): U8 => 0x0E
  fun string(): String iso^ => "BsonTypeDeprecated".clone()
primitive BsonTypeJSCodeWithScope
  fun apply(): U8 => 0x0F
  fun string(): String iso^ => "BsonTypeJSCodeWithScope".clone()
primitive BsonTypeInt32
  fun apply(): U8 => 0x10
  fun string(): String iso^ => "BsonTypeInt32".clone()
primitive BsonTypeTimestamp
  fun apply(): U8 => 0x11
  fun string(): String iso^ => "BsonTypeTimestamp".clone()
primitive BsonTypeInt64
  fun apply(): U8 => 0x12
  fun string(): String iso^ => "BsonTypeInt64".clone()
primitive BsonTypeMaximumKey
  fun apply(): U8 => 0x7F
  fun string(): String iso^ => "BsonTypeMaximumKey".clone()
primitive BsonTypeMinimumKey
  fun apply(): U8 => 0xFF
  fun string(): String iso^ => "BsonTypeMinimumKey".clone()

type BsonType is
( BsonTypeUnknown
| BsonTypeDouble
| BsonTypeStringUTF8
| BsonTypeDocument
| BsonTypeArray
| BsonTypeBinary
| BsonTypeUndefined
| BsonTypeOid
| BsonTypeBool
| BsonTypeTimeUTC
| BsonTypeNull
| BsonTypeRegexp
| BsonTypeDBPointer
| BsonTypeJSCode
| BsonTypeDeprecated
| BsonTypeJSCodeWithScope
| BsonTypeInt32
| BsonTypeTimestamp
| BsonTypeInt64
| BsonTypeMaximumKey
| BsonTypeMinimumKey
)

// -- Bson sub types
primitive BsonSubTypeGeneric
  fun apply(): U8 => 0x00
  fun string(): String iso^ => "BsonSubTypeGeneric".clone()
primitive BsonSubTypeFunction
  fun apply(): U8 => 0x01
  fun string(): String iso^ => "BsonSubTypeFunction".clone()
primitive BsonSubTypeBinaryOld
  fun apply(): U8 => 0x02
  fun string(): String iso^ => "BsonSubTypeBinaryOld".clone()
primitive BsonSubTypeUuidOld
  fun apply(): U8 => 0x03
  fun string(): String iso^ => "BsonSubTypeUuidOld".clone()
primitive BsonSubTypeUuid
  fun apply(): U8 => 0x04
  fun string(): String iso^ => "BsonSubTypeUuid".clone()
primitive BsonSubTypeMd5
  fun apply(): U8 => 0x05
  fun string(): String iso^ => "BsonSubTypeMd5".clone()
primitive BsonSubTypeUserDefined
  fun apply(): U8 => 0x80
  fun string(): String iso^ => "BsonSubTypeUserDefined".clone()

type BsonSubType is
( BsonSubTypeGeneric
| BsonSubTypeFunction
| BsonSubTypeBinaryOld
| BsonSubTypeUuidOld
| BsonSubTypeUuid
| BsonSubTypeMd5
| BsonSubTypeUserDefined
)

class BsonTimestampData
  let increment: I32
  let timestamp: I32

  new create(increment': I32, timestamp': I32) =>
    increment = increment'
    timestamp = timestamp'

class BsonBinaryData
  let sub_type: BsonSubType
  let value: String

  new create(sub_type': BsonSubType, value': String) =>
    sub_type = sub_type'
    value = value'

class BsonRegexpData
  let regex: String
  let options: String

  new create(regex': String, options': String) =>
    regex = regex'
    options = options'

class BsonDBPointerData
  let ref_col: String
  let ref_oid: ObjectID val

  new create(ref_col': String, ref_oid': ObjectID val) =>
    ref_col = ref_col'
    ref_oid = ref_oid'

type BsonPrimitiveData is
( ObjectID val
| BsonDBPointerData val
| BsonRegexpData val
| BsonBinaryData val
| BsonTimestampData val
| String val
| F64 val
| I64 val
| I32 val
| PosixDate val
| None val
| Bool val
)
type BsonData is (BsonPrimitiveData val | Map[String val, Bson val] val | Array[Bson val] val)

class Bson
  let kind: BsonType
  let data: BsonData val

  new create(kind': BsonType, data': BsonData val) =>
    kind = kind'
    data = data'
  
  fun eq(that: box->Bson): Bool =>
    if not (kind is that.kind) then
      return false
    end
    try
      match kind
      | BsonTypeOid => return (data as ObjectID val) == (that.data as ObjectID val)
      | BsonTypeBool => return (data as Bool val) == (that.data as Bool val)
      | BsonTypeStringUTF8 => return (data as String val) == (that.data as String val)
      | BsonTypeJSCode => return (data as String val) == (that.data as String val)
      | BsonTypeTimeUTC => return (data as PosixDate val).time() == (that.data as PosixDate val).time()
      | BsonTypeDBPointer =>
        let thisData = (data as BsonDBPointerData val)
        let thatData = (that.data as BsonDBPointerData val)
        return (thisData.ref_col == thatData.ref_col) and (thisData.ref_oid == thatData.ref_oid)
      | BsonTypeRegexp =>
        let thisData = (data as BsonRegexpData val)
        let thatData = (that.data as BsonRegexpData val)
        return (thisData.regex == thatData.regex) and (thisData.options == thatData.options)
      | BsonTypeTimestamp =>
        let thisData = (data as BsonTimestampData val)
        let thatData = (that.data as BsonTimestampData val)
        return (thisData.increment == thatData.increment) and (thisData.timestamp == thatData.timestamp)
      | BsonTypeArray =>
        let thisData = (data as Array[Bson val] val)
        let thatData = (that.data as Array[Bson val] val)
        if thisData.size() != thatData.size() then
          return false
        end
        var count: USize = 0
        while (count = count + 1) < 12 do
          try
            if thisData(count - 1)? != thatData(count - 1)? then return false end
          else
            return false
          end
        end
      | BsonTypeDocument =>
        let thisData = (data as Map[String val, Bson val] val)
        let thatData = (that.data as Map[String val, Bson val] val)
        if thisData.size() != thatData.size() then
          return false
        end
        for (key, value) in thisData.pairs() do
          try
            if value != thatData(key)? then return false end
          else
            return false
          end
        end
      end
    else
      return false
    end
    true
  
  fun ne(that: box->Bson): Bool =>
    not eq(that)
  
  fun string(): String iso^ =>
    to_json_string()
  fun to_json_string(): String iso^ =>
    let s: String ref = recover String end
    try
      match kind
      | BsonTypeMinimumKey => s.append("\"MinKey\"")
      | BsonTypeMaximumKey => s.append("\"MaxKey\"")
      | BsonTypeNull => s.append("null")
      | BsonTypeUndefined => s.append("\"undefined\"")
      | BsonTypeInt32 => s.append(Format.int[I32](data as I32 val))
      | BsonTypeInt64 => s.append(Format.int[I64](data as I64 val))
      | BsonTypeDouble => s.append(Format.float[F64](data as F64 val))
      | BsonTypeBool => s.append(if (data as Bool val) then "true" else "false" end)
      | BsonTypeDBPointer =>
        let doc = data as BsonDBPointerData val
        s.append("{\"ref_col\":\"" + doc.ref_col + "\",\"$oid\":\"" + doc.ref_oid.hex() + "\"}")
      | BsonTypeRegexp =>
        let doc = data as BsonRegexpData val
        s.append("{\"regex\":\"" + doc.regex + "\",\"options\":\"" + doc.options + "\"}")
      | BsonTypeTimestamp =>
        let doc = data as BsonTimestampData val
        s.append("{\"increment\":" + Format.int[I32](doc.increment) + ",\"timestamp\":" + Format.int[I32](doc.timestamp) + "}")
      | BsonTypeTimeUTC => s.append("{\"$date\":\"" + ((data as PosixDate val).format("%Y-%m-%dT%H:%M:%S")?) + "\"}")
      | BsonTypeOid => s.append("{\"$oid\":\"" + ((data as ObjectID val).hex()) + "\"}")
      | BsonTypeStringUTF8 => s.append("\"" + (data as String val) + "\"")
      | BsonTypeJSCode => s.append("\"" + (data as String val) + "\"")
      | BsonTypeArray =>
        let docs = data as Array[Bson val] val
        let vals: Array[String val] ref = recover Array[String val] end
        s.append("[")
        for doc in docs.values() do
          vals.push(doc.to_json_string())
        end
        s.append(", ".join(vals.values()))
        s.append("]")
      | BsonTypeDocument =>
        let docs = data as Map[String val, Bson val] val
        let vals: Array[String val] ref = recover Array[String val] end
        s.append("{")
        for (key, doc) in docs.pairs() do
          vals.push("\"" + key + "\":" + doc.to_json_string())
        end
        s.append(", ".join(vals.values()))
        s.append("}")
      end
    end
    s.clone()

  fun bytes(): Array[U8] val =>
    let buf: Writer = Writer
    var offset: I32 = 0
    match kind
    | BsonTypeOid => try buf.write((data as ObjectID val).bytes()) end
    // -- write nothing?
    // | BsonTypeNull
    // | BsonTypeUndefined
    // | BsonTypeMaximumKey
    // | BsonTypeMinimumKey
    // | BsonTypeUnknown
    // | BsonTypeDeprecated
    // | BsonTypeJSCodeWithScope
    | BsonTypeDouble => try buf.f64_le(data as F64 val) end
    | BsonTypeStringUTF8 => 
      try
        buf.write((data as String val).array())
        buf.u8(0x00)
      end
    | BsonTypeDocument =>
      try
        let doc = data as (Map[String val, Bson val] val)
        for (key, value) in doc.pairs() do
          buf.u8(value.kind())
          buf.write(key.array())
          buf.u8(0x00)
          buf.write(value.bytes())
        end
        buf.u8(0x00)
        offset = I32(0).bytewidth().i32()
      end
    | BsonTypeArray =>
      try
        let doc = data as (Array[Bson val] val)
        for (idx, value) in doc.pairs() do
          buf.u8(value.kind())
          buf.write(Format.int[USize](idx))
          buf.u8(0x00)
          buf.write(value.bytes())
        end
        buf.u8(0x00)
        offset = I32(0).bytewidth().i32()
      end
    | BsonTypeBinary =>
      try
        let doc = data as BsonBinaryData val
        match doc.sub_type
        | BsonSubTypeMd5 =>
            buf.i32_le(16)
            let digest: Array[U8] iso = recover Array[U8].init(0, 16) end
            digest.copy_from(doc.value.array(), 0, 0, 16)
            buf.u8(doc.sub_type())
            buf.write(consume digest)
        else
          buf.i32_le(doc.value.size().i32())
          buf.u8(doc.sub_type())
          buf.write(doc.value.array())
        end
      end
    | BsonTypeBool =>
      try
        let doc = data as Bool val
        buf.u8(if doc then 1 else 0 end)
      end
    | BsonTypeTimeUTC =>
      try
        let doc = data as PosixDate val
        buf.i64_le(doc.time() * 1000)
      end
    | BsonTypeRegexp =>
      try
        let doc = data as BsonRegexpData val
        buf.write(doc.regex.array())
        buf.u8(0x00)
        buf.write(doc.options.array())
        buf.u8(0x00)
      end
    | BsonTypeDBPointer =>
      try
        let doc = data as BsonDBPointerData val
        buf.i32_le((doc.ref_col.size() + 1).i32())
        buf.write(doc.ref_col.array())
        buf.u8(0x00)
        buf.write(doc.ref_oid.bytes())
      end
    | BsonTypeJSCode =>
      try
        buf.write((data as String val).array())
        buf.u8(0x00)
      end
    | BsonTypeInt32 =>
      try
        let doc = data as I32 val
        buf.i32_le(doc)
      end
    | BsonTypeTimestamp =>
      try
        let doc = data as BsonTimestampData val
        buf.i32_le(doc.increment)
        buf.i32_le(doc.timestamp)
      end
    | BsonTypeInt64 =>
      try
        let doc = data as I64 val
        buf.i64_le(doc)
      end
    end

    let tmp: Array[U8] iso = Array[U8](0)
    for chunk in buf.done().values() do
      tmp.append(chunk)
    end

    match kind
    | BsonTypeDocument => _with_size_offset(consume tmp, offset)
    | BsonTypeArray => _with_size_offset(consume tmp, offset)
    | BsonTypeStringUTF8 => _with_size_offset(consume tmp, offset)
    | BsonTypeJSCode => _with_size_offset(consume tmp, offset)
    else
      consume val tmp
    end
  
  fun _with_size_offset(result: Array[U8] iso, offset: I32): Array[U8] val =>
    let buf: Writer = Writer
    buf.reserve_current(I32(0).bytewidth())
    buf.i32_le(result.size().i32() + offset)
    try
      let sizechunk = buf.done()(0)?
      match sizechunk
      | let s: Array[U8] val => 
        for byte in s.reverse().values() do
          result.unshift(byte)
        end
      end
    end
    consume val result
