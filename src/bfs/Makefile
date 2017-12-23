PROJECT = bfs
GO = go
DEP = dep
PACKAGES = . $(PROJECT)/volume $(PROJECT)/block $(PROJECT)/ns
PROJECT_DIRS = . volume block ns

.PHONEY: help all compile clean check format

help:
	@echo "Supported targets for $(PROJECT):\n\
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
	$(DEP) ensure
	$(GO) install

clean:
	rm -rf $(GOPATH)/pkg $(GOPATH)/bin
	for p in $(PROJECT_DIRS); do \
		if [ -d "$$p/build" ] ; then \
			rm -rf "$$p/build"; \
		fi \
	done

realclean: clean
	rm -rf ./vendor

check: compile
	$(GO) test $(TEST_ARGS) $(PACKAGES) $(TEST_EXTRA_ARGS)

format:
	$(GO) fmt $(PACKAGES)

