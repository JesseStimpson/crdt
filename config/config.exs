import Config

config :riak_core,
  # The vnode manager has an internal tick interval that it uses to check for
  # various actions to take on the vnodes, including ownership transfer. By
  # setting a low value during development, we remove the default 10sec wait
  # for vnodes to join the cluster.
  vnode_management_timer: 100,

  # Handoffs are executed concurrently between nodes according to this limit.
  # During development, a high value means the ring will settle more quickly.
  # If handoff is expensive, you want to lower this value dramatically.
  # Default is 2.
  handoff_concurrency: 100,

  # The interval at which a physical node will broadcast its existence to
  # other reachable physical nodes. Default 60000
  gossip_interval: 100,

  # The interval at which a vnode will report itself as inactive to the manager.
  # The manager uses this opportunity to give the vnode new work, like hinted handoffs,
  # if any are pending. Default 60000
  vnode_inactivity_timeout: 100,

  # The number of vnodes. Default 64.
  ring_creation_size: 64
