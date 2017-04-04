.PHONY: default clean

CLI_NAME = mongoes
OS := $(shell uname)
VERSION ?= 1.0.0

# test target

# target #

default: clean build_mongoes

build_mongoes: 
	@echo "Setup Mongoes"
ifeq ($(OS),Linux)
	mkdir -p build/linux
	@echo "Build Mongoes..."
	GOOS=linux  go build -ldflags "-s -w -X main.Version=$(VERSION)" -o build/linux/$(CLI_NAME) cmd/main.go
endif
ifeq ($(OS) ,Darwin)
	@echo "Build Mongoes..."
	GOOS=darwin go build -ldflags "-X main.Version=$(VERSION)" -o build/mac/$(CLI_NAME) cmd/main.go
endif
	@echo "Succesfully Build for ${OS} version:= ${VERSION}"

install:
	echo "Install Mongoes, ${OS} version:= ${VERSION}"
ifeq ($(OS),Linux)
	mv build/linux/$(CLI_NAME) /usr/local/bin/$(CLI_NAME)
endif
ifeq ($(OS) ,Darwin)
	mv build/darwin/$(CLI_NAME) /usr/local/bin/$(CLI_NAME)
endif

clean:
	rm -rf build/*