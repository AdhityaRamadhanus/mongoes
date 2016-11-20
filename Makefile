.PHONY: clean
.PHONY: test

# Flags #
GO_FLAGS = -o

# Path configuration #
GONDEX_DIR = gondex
GOWATCH_DIR = gowatch
BIN_DIR = bin
# Harcoded bro
TEST_PKG = github.com/AdhityaRamadhanus/mongoes/mongoes_test 

# target #

default: test clean build_gondex

build_gowatch: $(GOWATCH_DIR)/main.go
	cd $(GOWATCH_DIR); \
	go build $(GO_FLAGS) $(BIN_DIR)/gowatch.exe; \
	cd ..; \

build_gondex: $(GONDEX_DIR)/main.go
	cd $(GONDEX_DIR); \
	go build $(GO_FLAGS) $(BIN_DIR)/gondex.exe; \
	cd ..; \


clean:
	rm -rf $(GONDEX_DIR)/*.exe
	rm -rf $(GONDEX_DIR)/$(BIN_DIR)
	rm -rf $(GOWATCH_DIR)/*.exe
	rm -rf $(GOWATCH_DIR)/$(BIN_DIR)

test:
	go test $(TEST_PKG)
