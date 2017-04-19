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
	sudo cp ./build/linux/mongoes /usr/local/bin/
endif
ifeq ($(OS) ,Darwin)
	@echo "Build Mongoes..."
	GOOS=darwin go build -ldflags "-X main.Version=$(VERSION)" -o build/mac/$(CLI_NAME) cmd/main.go
	sudo cp ./build/mac/mongoes /usr/local/bin/
endif
	@echo "Succesfully Build and Installed for ${OS} version:= ${VERSION}"

clean:
	rm -rf build/*