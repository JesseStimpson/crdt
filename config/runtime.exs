import Config

# config/runtime.exs is executed for all environments, including
# during releases. It is executed after compilation and before the
# system starts, so it is typically used to load production configuration
# and secrets from environment variables or elsewhere. Do not define
# any compile-time configuration in here, as it won't be applied.
# The block below contains prod specific runtime configuration.

config :setup,
  data_dir: (System.get_env("CRDT_PLATFORM_DATA_DIR") || "data") <> "/setup",
  log_dir: (System.get_env("CRDT_PLATFORM_DATA_DIR") || "data") <> "/log"

config :riak_core,
  web_port: String.to_integer(System.get_env("CRDT_WEB_PORT") || "8198"),
  handoff_port: String.to_integer(System.get_env("CRDT_HANDOFF_PORT") || "8199"),
  ring_state_dir: to_charlist(System.get_env("CRDT_RING_STATE_DIR") || "data/ring"),
  platform_data_dir: to_charlist(System.get_env("CRDT_PLATFORM_DATA_DIR") || "data")
