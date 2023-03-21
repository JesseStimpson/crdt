defmodule CrdtTest do
  use ExUnit.Case
  doctest Crdt

  setup_all do

    env_common = [
      vnode_management_timer: 100,
      handoff_concurrency: 100,
      gossip_interval: 100,
      vnode_inactivity_timeout: 100,
      ring_creation_size: 64
    ]

    env_node = fn idx, name ->
      [
        web_port: 9098 + (idx*100),
        handoff_port: 9099 + (idx*100),
        ring_state_dir: 'data/' ++ Atom.to_charlist(name),
        platform_data_dir: 'data/' ++ Atom.to_charlist(name),
      ]
    end

    node_names = [:testa, :testb, :testc]

    nodes = [testa, testb, testc] = for {idx, name} <- Enum.zip(1..3, node_names) do
      env = env_common ++ env_node.(idx, name)
      # LocalCluster applications and environment options do not behave well. Specifically,
      # env vars from the app file override the passed in environment, unexpectedly.
      [node] = LocalCluster.start_nodes(name, 1, applications: [], environment: [])
      :pong = Node.ping(node)
      :rpc.call(node, Application, :load, [:riak_core])
      for {k, v} <- env, do: :rpc.call(node, Application, :put_env, [:riak_core, k, v])
      {:ok, _} = :rpc.call(node, Application, :ensure_all_started, [:crdt])
      {:ok, _ring} = :rpc.call(node, :riak_core_ring_manager, :get_my_ring, [])
      node
    end

    {_, []} = :rpc.multicall([testb, testc], :riak_core, :join, [testa])

    :ok = await_handoffs(testa, 0, 5000)

    on_exit(fn ->
      :ok = LocalCluster.stop()
    end)

    rpca = &(:rpc.call(testa, &1, &2, &3))
    rpcb = &(:rpc.call(testb, &1, &2, &3))
    rpcc = &(:rpc.call(testc, &1, &2, &3))
    %{nodes: nodes,
      rpc: [rpca, rpcb, rpcc]}
  end

  defp await_handoffs(_node, _depth, timeout) when timeout <= 0 do
    {:error, :timeout}
  end
  defp await_handoffs(node, depth, timeout) do
    case :rpc.call(node, :riak_core_handoff_manager, :status, []) do
      [] ->
        :ok
      _ ->
        :timer.sleep(10)
        await_handoffs(node, depth+1, timeout-10)
    end
  end

  test "greets the world" do
    assert Crdt.hello() == :world
  end

  test "pongs", context do
    %{nodes: [testa, testb, testc]} = context
    assert Node.ping(testa) == :pong
    assert Node.ping(testb) == :pong
    assert Node.ping(testc) == :pong
  end

  test "incr test", %{rpc: [rpc|_]} do
    name = rpc.(Crdt, :do_test, [])
    {:ok, %{a: %{c: 1}}} = rpc.(Crdt, :get, [name])
  end
end
