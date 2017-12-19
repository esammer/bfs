
.PHONEY = all compile clean check format

GO = go

all: compile check

compile:
	$(GO) install

clean:
	rm -rf $(GOPATH)/pkg $(GOPATH)/bin

check: compile
	$(GO) test -v

format:
	$(GO) fmt

