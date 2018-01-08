PROJECT = bfs
GO = go
DEP = dep
PACKAGES = . \
	$(PROJECT)/block \
	$(PROJECT)/blockservice \
	$(PROJECT)/fsservice \
	$(PROJECT)/host \
	$(PROJECT)/lru \
	$(PROJECT)/nameservice \
	$(PROJECT)/ns \
	$(PROJECT)/server/blockserver \
	$(PROJECT)/server/nameserver \
	$(PROJECT)/test \
	$(PROJECT)/util/size \
	$(PROJECT)/volume \
	$(PROJECT)/volumeutil
PROJECT_DIRS = . \
	block \
	blockservice \
	fsservice \
	host \
	lru \
	nameservice \
	ns \
	server/blockserver \
	server/nameserver \
	test \
	util/size \
	volume \
	volumeutil
PROTO_FILES = blockservice/blockservice.pb.go nameservice/nameservice.pb.go fsservice/fsservice.pb.go

.PHONY: all benchmark check clean compile deps format generate generate-proto help realclean

all: compile check

help:
	@echo "Supported targets for $(PROJECT):\n\
	\n\
	  Standard:\n\
	    all       run compile and check.\n\
	    check     run tests for all packages.\n\
	    benchmark run benchmarks for all packages.\n\
	    clean     remove all generated build artifacts.\n\
	    compile   compile and install all source in $(GOPATH).\n\
	    help      display this help.\n\
	\n\
	  Developer:\n\
	    deps      ensure/build required dependencies.\n\
	    format    run $(GO) fmt on all source.\n\
	    generate  generate required source files.\n\
	"
	@if test -z "$(GOPATH)"; then \
		echo "***WARNING*** GOPATH is not set!"; \
	fi

deps:
	$(DEP) ensure

compile: deps generate-proto
	$(GO) install

check: compile
	$(GO) test $(TEST_ARGS) $(PACKAGES) $(TEST_EXTRA_ARGS)

benchmark: compile
	$(GO) test $(TEST_ARGS) $(PACKAGES) -bench . -benchmem -run ^$$ $(TEST_EXTRA_ARGS)

generate: generate-proto

generate-proto: $(PROTO_FILES)

clean:
	rm -rf $(GOPATH)/pkg/*/bfs $(GOPATH)/bin/bfs
	for p in $(PROJECT_DIRS); do \
		if [ -d "$$p/build" ] ; then \
			rm -rf "$$p/build"; \
		fi \
	done

realclean: clean
	rm -rf ./vendor
	rm -rf $(PROTO_FILES)

format:
	$(GO) fmt $(PACKAGES)

%.pb.go: %.proto
	protoc $< --go_out=plugins=grpc:.
