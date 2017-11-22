
noop=
space= $(noop) $(noop)
comma= ,

PACKAGE=github.com/kphelps/actors

PROJECT_VENDOR=$(PACKAGE)/vendor/

ALL_SRC=$(wildcard **/*.go *.go $(GO_PROTO_SRC))
TEST_SRC=$(wildcard **/*_test.go *_test.go)
MOCK_SRC=$(wildcard mocks/*.go)
EXCLUDE_SRC=$(TEST_SRC) $(MOCK_SRC)
GO_SRC=$(filter-out $(EXCLUDE_SRC), $(ALL_SRC))

MOCK_PACKAGES= \
	actors

MOCK_FILES=$(addsuffix /mocks.go,$(addprefix mocks/,$(MOCK_PACKAGES)))

actors_MOCK_INTERFACES= \
	Actor \
	ActorContext \
	ActorRef \
	ReadSideHandler \
	SequenceTracker

.PHONY: build cover mocks test

build: $(GO_SRC)
	@go build ./...

$(MOCK_FILES): mocks/%/mocks.go: $(GO_SRC)
	@mkdir -p $$(dirname $@)
	@mockgen -package $*_mocks \
		-destination $@ \
		$(or $($*_PACKAGE),$(PACKAGE)/$*) \
		$(subst $(space),$(comma),$($*_MOCK_INTERFACES))
	@sed 's|$(PROJECT_VENDOR)||' $@ > $@.tmp.bak && mv $@.tmp.bak $@

mocks: $(MOCK_FILES)

test: $(GO_SRC) mocks
	@ginkgo -r

cover: $(GO_SRC) mocks
	@ginkgo -cover -r
	@gover
	@go tool cover -html=gover.coverprofile
