.PHONY: iex
iex:
	CRDT_RING_STATE_DIR=data/crdt/ring CRDT_PLATFORM_DATA_DIR=data/crdt CRDT_WEB_PORT=8198 CRDT_HANDOFF_PORT=8199 iex --erl "-name crdt@127.0.0.1" -S mix run

.PHONY: iex2
iex2:
	CRDT_RING_STATE_DIR=data/crdt2/ring CRDT_PLATFORM_DATA_DIR=data/crdt2 CRDT_WEB_PORT=8298 CRDT_HANDOFF_PORT=8299 iex --erl "-name crdt2@127.0.0.1" -S mix run
