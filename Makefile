PROJECT                   := github.com/usherasnick/Useful-Go-Gadgets
SRC                       := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
ELECTLEADER_TARGETS       := z_node_01 z_node_02 z_node_03
GOCQL_ELECTLEADER_TARGETS := c_node_01 c_node_02 c_node_03
ALL_TARGETS               := $(ELECTLEADER_TARGETS) $(GOCQL_ELECTLEADER_TARGETS)

all: build

build: $(ALL_TARGETS)

$(ELECTLEADER_TARGETS): $(SRC)
	go build $(GOMODULEPATH)/$(PROJECT)/cmd/leaderelect/$@

$(GOCQL_ELECTLEADER_TARGETS): $(SRC)
	go build $(GOMODULEPATH)/$(PROJECT)/cmd/gocql-leaderelect/$@

test:
	go test -count=1 -v -p 1 $(shell go list ./...)

clean:
	rm -f $(ALL_TARGETS)

.PHONY: all build clean
