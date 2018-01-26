PROJECT = bfs
GO = go
DEP = dep
PACKAGES = . \
	$(PROJECT)/block \
	$(PROJECT)/blockservice \
	$(PROJECT)/client \
	$(PROJECT)/config \
	$(PROJECT)/file \
	$(PROJECT)/lru \
	$(PROJECT)/nameservice \
	$(PROJECT)/ns \
	$(PROJECT)/selector \
	$(PROJECT)/server/blockserver \
	$(PROJECT)/server/nameserver \
	$(PROJECT)/test \
	$(PROJECT)/util/size \
	$(PROJECT)/volumeutil
PROJECT_DIRS = . \
	block \
	blockservice \
	client \
	config \
	file \
	lru \
	nameservice \
	ns \
	selector \
	server/blockserver \
	server/nameserver \
	test \
	util/size \
	volumeutil
PROTO_FILES = \
	blockservice/blockservice.pb.go \
	config/config.pb.go \
	nameservice/nameservice.pb.go

ifdef v
VERBOSE = -v
endif

.PHONY: all benchmark check clean compile deps format generate generate-proto help realclean vet

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
	    vet       run $(GO) vet on all source.\n\
	"
	@if test -z "$(GOPATH)"; then \
		echo "***WARNING*** GOPATH is not set!"; \
	fi

deps:
	$(DEP) ensure $(VERBOSE)

compile: deps generate-proto
	$(GO) install $(VERBOSE)

check: compile
	$(GO) test $(VERBOSE) $(TEST_ARGS) $(PACKAGES) $(TEST_EXTRA_ARGS)

benchmark: compile
	mkdir -p benchmarks
	sh -c 'bench_file_name="benchmarks/$$(date +%Y%m%d-%H%M%S).txt"; \
		$(GO) test $(VERBOSE) $(TEST_ARGS) $(PACKAGES) -bench . -benchmem -run ^$$ $(TEST_EXTRA_ARGS) | tee $$bench_file_name'
	find benchmarks/ -type f -a \! -name latest_change.txt -print | sort -r | head -n 2 | xargs benchcmp -changed | tee benchmarks/latest_change.txt

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

vet:
	$(GO) vet $(VERBOSE) $(PACKAGES)

%.pb.go: %.proto
	protoc $< --go_out=plugins=grpc:..
