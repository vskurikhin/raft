include .env

PROJECTNAME=$(shell basename "$(PWD)")

# Go related variables.
GOBASE=$(shell pwd)
GOPATH="$(GOBASE)/vendor:$(GOBASE)"
CMD_SERVER=cmd
GOBIN=$(GOBASE)/$(CMD_SERVER)
GOFILES=$(wildcard *.go)

# Redirect error output to a file, so we can show it in development mode.
STDERR=/tmp/.$(PROJECTNAME)kv-stderr.txt

# PID file will keep the process id of the raft
PID_RAFT1=/tmp/.$(PROJECTNAME)kv-1.pid
PID_RAFT2=/tmp/.$(PROJECTNAME)kv-2.pid
PID_RAFT3=/tmp/.$(PROJECTNAME)kv-3.pid
PID_LOADKV=/tmp/.loadkv.pid

TRACE_LOG_RAFT1=$(GOBASE)/$(PROJECTNAME)kv-1.trace
TRACE_LOG_RAFT2=$(GOBASE)/$(PROJECTNAME)kv-2.trace
TRACE_LOG_RAFT3=$(GOBASE)/$(PROJECTNAME)kv-3.trace

STD_ERR_RAFT1=$(GOBASE)/$(PROJECTNAME)kv-1.stderr
STD_ERR_RAFT2=$(GOBASE)/$(PROJECTNAME)kv-2.stderr
STD_ERR_RAFT3=$(GOBASE)/$(PROJECTNAME)kv-3.stderr

HTTP1_PORT=8881
HTTP2_PORT=8882
HTTP3_PORT=8883

RAFT1_PORT=9991
RAFT2_PORT=9992
RAFT3_PORT=9993

TEMP_FILE=$(shell mktemp)
MAIN_GO=./$(CMD_SERVER)/main.go

# Make is verbose in Linux. Make it silent.
MAKEFLAGS += --silent

## coverage: Calculate coverage
coverage: go-coverage

## start: Start in development mode. Auto-starts when code changes.
start: start-raft

## stop: Stop development mode. RAFT
stop: stop-raft

start-raft: stop-raft
	@echo "  >  $(PROJECTNAME) is available at $(HTTP1_PORT)"
	@-$(GOBIN)/$(PROJECTNAME)kv -number 1 -http-addr=":$(HTTP1_PORT)" -rpc-addr=":$(RAFT1_PORT)" -peers="2=:$(RAFT2_PORT),3=:$(RAFT3_PORT)" -trace-log-level 10 --trace-log-file "$(TRACE_LOG_RAFT1)" 2>"$(STD_ERR_RAFT1)" & echo $$! > $(PID_RAFT1)
	@cat $(PID_RAFT1) | sed "/^/s/^/  \>  PID1: /"
	@echo "  >  $(PROJECTNAME) is available at $(HTTP2_PORT)"
	@-$(GOBIN)/$(PROJECTNAME)kv -number 2 -http-addr=":$(HTTP2_PORT)" -rpc-addr=":$(RAFT2_PORT)" -peers="1=:$(RAFT1_PORT),3=:$(RAFT3_PORT)" -trace-log-level 10 --trace-log-file "$(TRACE_LOG_RAFT2)" 2>"$(STD_ERR_RAFT2)" & echo $$! > $(PID_RAFT2)
	@cat $(PID_RAFT2) | sed "/^/s/^/  \>  PID2: /"
	@echo "  >  $(PROJECTNAME) is available at $(HTTP3_PORT)"
	@-$(GOBIN)/$(PROJECTNAME)kv -number 3 -http-addr=":$(HTTP3_PORT)" -rpc-addr=":$(RAFT3_PORT)" -peers="1=:$(RAFT1_PORT),2=:$(RAFT2_PORT)" -trace-log-level 10 --trace-log-file "$(TRACE_LOG_RAFT3)" 2>"$(STD_ERR_RAFT3)" & echo $$! > $(PID_RAFT3)
	@cat $(PID_RAFT3) | sed "/^/s/^/  \>  PID3: /"
	@-$(GOBIN)/loadkv -get-percent 75 -peers ":$(HTTP1_PORT),:$(HTTP2_PORT),:$(HTTP3_PORT)" -request-rate 1000 > ./loadkv-1.out 2>&1  & echo $$! > $(PID_LOADKV)
	@cat $(PID_RAFT3) | sed "/^/s/^/  \>  PID3: /"

stop-raft:
	@-touch $(PID_LOADKV)
	@-kill `cat $(PID_LOADKV)` 2> /dev/null || true
	@-rm $(PID_LOADKV)
	@-touch $(PID_RAFT3)
	@-kill `cat $(PID_RAFT3)` 2> /dev/null || true
	@-rm $(PID_RAFT3)
	@-touch $(PID_RAFT2)
	@-kill `cat $(PID_RAFT2)` 2> /dev/null || true
	@-rm $(PID_RAFT2)
	@-touch $(PID_RAFT1)
	@-kill `cat $(PID_RAFT1)` 2> /dev/null || true
	@-rm $(PID_RAFT1)

restart-raft: stop-raft start-raft

## build: Compile the binary.
build: go-build-raft

## clean: Clean build files. Runs `go clean` internally.
clean: go-clean
	@-rm -rf ./loadkv-1.out
	@-rm -rf ./data
	@-rm "$(STD_ERR_RAFT3)"
	@-rm "$(STD_ERR_RAFT2)"
	@-rm "$(STD_ERR_RAFT1)"
	@-rm "$(TRACE_LOG_RAFT3)"
	@-rm "$(TRACE_LOG_RAFT2)"
	@-rm "$(TRACE_LOG_RAFT1)"
	@-rm $(GOBIN)/loadkv
	@-rm $(GOBIN)/$(PROJECTNAME)kv

go-compile: go-build-raft

go-build-raft:
	@echo "  >  Building $(PROJECTNAME)kv binary..."
	@GOPATH=$(GOPATH) GOBIN=$(GOBIN) cd $(CMD_SERVER) && go build -o $(GOBIN)/$(PROJECTNAME)kv $(GOFILES)
	@echo "  >  Building loadkv binary..."
	@GOPATH=$(GOPATH) GOBIN=$(GOBIN) cd $(CMD_SERVER)/load && go build -o $(GOBIN)/loadkv $(GOFILES)

# 1. Generate the list of packages to include, excluding 'mocks'
# go list ./... lists all packages; grep -v excludes lines containing 'mocks'
go-coverage:
	CVPKG=$(go list ./... | grep -v '/mocks' | tr '\n' ',')
	# 2. Run tests with coverage analysis applied only to the specified packages
	# @GOPATH=$(GOPATH) GOBIN=$(GOBIN) go test -coverpkg=$(CVPKG) -coverprofile=coverage.out ./...
	@GOPATH=$(GOPATH) GOBIN=$(GOBIN) go test -coverprofile=coverage.out ./...
	# 3. (Optional) Generate the HTML report
	@GOPATH=$(GOPATH) GOBIN=$(GOBIN) go tool cover -html=coverage.out -o docs/coverage.html

go-get:
	@echo "  >  Checking if there is any missing dependencies..."
	@GOPATH=$(GOPATH) GOBIN=$(GOBIN) go get $(get)

.PHONY: go-update-deps
go-update-deps:
	@echo ">> updating Go dependencies"
	@for m in $$(go list -mod=readonly -m -f '{{ if and (not .Indirect) (not .Main)}}{{.Path}}{{end}}' all); do \
		go get $$m; \
	done
	go mod tidy
ifneq (,$(wildcard vendor))
	go mod vendor
endif

go-install:
	@GOPATH=$(GOPATH) GOBIN=$(GOBIN) go install $(GOFILES)

go-clean:
	@echo "  >  Cleaning build cache"
	@GOPATH=$(GOPATH) GOBIN=$(GOBIN) go clean

.PHONY: help
all: help
help: Makefile
	@echo
	@echo " Choose a command run in "$(PROJECTNAME)":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo
