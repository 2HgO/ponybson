.PHONY: test
test:
	@corral run -- ponyc ponybson -o build -b bson && ./build/bson
