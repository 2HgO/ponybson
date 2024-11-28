use "format"

interface BsonError is Stringable
  fun string(): String iso^ =>
    "Unimplemented".clone()
  fun name(): String iso^ => string()
  fun details(): String iso^

type OrBsonError[A] is (BsonError val | A)

class GenericBsonError is BsonError
  let _description: String
  new create(description': String) =>
    _description = description'
  fun details(): String iso^ => _description.clone()

class UnknownBsonError is BsonError
  let _idx: USize
  new create(idx': USize) =>
    _idx = idx'
  fun string(): String iso^ => "UnknownBsonError".clone()
  fun details(): String iso^ => "illegal hex string at index: " + Format.int[USize](_idx)

class IllegalBsonTypeError is BsonError
  let _kind: BsonType
  let _idx: USize
  new create(kind': BsonType, idx': USize) =>
    _kind = kind'
    _idx = idx'
  fun string(): String iso^ => "IllegalBsonTypeError".clone()
  fun details(): String iso^ => "illegal bson type: ('" + _kind.string() + "') found at index: " + Format.int[USize](_idx)

class UnknownBsonTypeError is BsonError
  let _byte: U8
  let _idx: USize
  new create(byte': U8, idx': USize) =>
    _byte = byte'
    _idx = idx'
  fun string(): String iso^ => "UnknownBsonTypeError".clone()
  fun details(): String iso^ => "Unknown Bson type: ('" + Format.int[U8](_byte where fmt = FormatHex, fill = '0', width = 4, align = AlignLeft) + "') at index: " + Format.int[USize](_idx)

primitive IllegalObjectIDHexError is BsonError
  fun details(): String iso^ => "illegal hex string provided".clone()
  fun string(): String iso^ => "IllegalObjectIDHexError".clone()

primitive IllegalObjectIDError is BsonError
  fun details(): String iso^ => "illegal oid bytes provided".clone()
  fun string(): String iso^ => "IllegalObjectIDError".clone()
