use "pony_test"
use "time"

actor Main is TestList
  new create(env: Env) =>
    PonyTest(env, this)
  
  new make() => None

  fun tag tests(test: PonyTest) =>
    ObjectIDTests.make().tests(test)
    BsonUtilsTests.make().tests(test)
