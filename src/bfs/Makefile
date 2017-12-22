PROJECT = bfs
GO = go
PACKAGES = $(PROJECT)/volume $(PROJECT)/block $(PROJECT)/file

.PHONEY: help all compile clean check format

help:
	echo "Supported targets for $(PROJECT):\n\
	\n\
	  Standard:\n\
	    all       run compile and check.\n\
	    check     run tests for all packages.\n\
	    clean     remove all generated build artifacts.\n\
	    compile   compile and install all source in $(GOPATH).\n\
	    help      display this help.\n\
	\n\
	  Developer:\n\
	    format    run $(GO) fmt on all source.\n\
	"
	@if test -z "$(GOPATH)"; then \
		echo "***WARNING*** GOPATH is not set!"; \
	fi

all: compile check

compile:
	$(GO) install

clean:
	rm -rf $(GOPATH)/pkg $(GOPATH)/bin

check: compile
	$(GO) test -v $(TEST_ARGS) $(PACKAGES)

format:
	$(GO) fmt

