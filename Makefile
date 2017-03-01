.PHONY: clean
.PHONY: test

# Flags #
GO_FLAGS = -race -o

# Path configuration #
CMD_DIR = cmd
BIN_DIR = bin
# Harcoded bro
TEST_PKG = github.com/AdhityaRamadhanus/mongoes

# target #

default: test clean build_mongoes

build_mongoes: 
	cd $(CMD_DIR); \
	go build $(GO_FLAGS) $(BIN_DIR)/mongoes; \
	cd ../..; \

clean:
	rm -rf $(CMD_DIR)/$(BIN_DIR)/*

test:
	go test $(TEST_PKG)
