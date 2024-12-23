defmodule SampleUser do
  defstruct name: "alice", status: :online
end

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
        web_port: 9098 + idx * 100,
        handoff_port: 9099 + idx * 100,
        ring_state_dir: 'data/' ++ Atom.to_charlist(name),
        platform_data_dir: 'data/' ++ Atom.to_charlist(name)
      ]
    end

    node_names = [:testa, :testb, :testc]

    nodes =
      [testa, testb, testc] =
      for {idx, name} <- Enum.zip(1..3, node_names) do
        env = env_common ++ env_node.(idx, name)
        # LocalCluster applications and environment options do not behave well. Specifically,
        # env vars from the app file override the passed in environment, unexpectedly.
        {:ok, cluster} = LocalCluster.start_link(1, prefix: name, applications: [], environment: [])
        {:ok, [node]} = LocalCluster.nodes(cluster)
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

    rpca = &:rpc.call(testa, &1, &2, &3)
    rpcb = &:rpc.call(testb, &1, &2, &3)
    rpcc = &:rpc.call(testc, &1, &2, &3)
    %{nodes: nodes, rpc: [rpca, rpcb, rpcc]}
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
        await_handoffs(node, depth + 1, timeout - 10)
    end
  end

  test "pongs", context do
    %{nodes: [testa, testb, testc]} = context
    assert Node.ping(testa) == :pong
    assert Node.ping(testb) == :pong
    assert Node.ping(testc) == :pong
  end

  test "incr test", %{rpc: [rpc | _]} do
    name = rpc.(Crdt, :do_test, [])
    {:ok, %{a: %{c: 1}}} = rpc.(Crdt, :get, [name])
  end

  test "incr api test", %{rpc: [rpc | _]} do
    pipe = fn x, m, f, a ->
      rpc.(m, f, [x | a])
    end

    :ok =
      rpc.(Crdt, :for, [:incr_api])
      |> pipe.(Crdt, :at, [:latest])
      |> pipe.(Crdt, :increment, [:like])
      |> pipe.(Crdt, :apply, [])

    {:ok, %{latest: %{like: 1}}} = rpc.(Crdt, :get, [:incr_api])

    :ok =
      rpc.(Crdt, :for, [:incr_api])
      |> pipe.(Crdt, :at, [:latest])
      |> pipe.(Crdt, :decrement, [:like])
      |> pipe.(Crdt, :apply, [])

    {:ok, %{latest: %{like: 0}}} = rpc.(Crdt, :get, [:incr_api])
  end

  test "lwwreg api test", %{rpc: [rpc | _]} do
    pipe = fn x, m, f, a ->
      rpc.(m, f, [x | a])
    end

    :ok =
      rpc.(Crdt, :for, [:lwwreg_api])
      |> pipe.(Crdt, :at, [:latest])
      |> pipe.(Crdt, :register, [:title, {"Hello", "World"}])
      |> pipe.(Crdt, :apply, [])

    {:ok, %{latest: %{title: {"Hello", "World"}}}} = rpc.(Crdt, :get, [:lwwreg_api])
  end

  test "orswot api test", %{rpc: [rpc | _]} do
    pipe = fn x, m, f, a ->
      rpc.(m, f, [x | a])
    end

    :ok =
      rpc.(Crdt, :for, [:orswot_api])
      |> pipe.(Crdt, :at, [:latest])
      |> pipe.(Crdt, :insert, [:followers, :bob])
      |> pipe.(Crdt, :apply, [])

    :ok =
      rpc.(Crdt, :for, [:orswot_api])
      |> pipe.(Crdt, :at, [:latest])
      |> pipe.(Crdt, :insert, [:followers, :alice])
      |> pipe.(Crdt, :apply, [])

    {:ok, %{latest: %{followers: [:alice, :bob]}}} = rpc.(Crdt, :get, [:orswot_api])

    :ok =
      rpc.(Crdt, :for, [:orswot_api])
      |> pipe.(Crdt, :at, [:latest])
      |> pipe.(Crdt, :remove, [:followers, :alice])
      |> pipe.(Crdt, :apply, [])

    {:ok, %{latest: %{followers: [:bob]}}} = rpc.(Crdt, :get, [:orswot_api])
  end

  test "struct test", %{rpc: [rpc | _]} do
    pipe = fn x, m, f, a ->
      rpc.(m, f, [x | a])
    end

    # register_struct will register all kvs of that struct with the crdt
    :ok =
      rpc.(Crdt, :for, [:struct_api])
      |> pipe.(Crdt, :at, [:user])
      |> pipe.(Crdt, :register_struct, [%SampleUser{}])
      |> pipe.(Crdt, :apply, [])

    {:ok, %{user: %SampleUser{}}} = rpc.(Crdt, :get, [:struct_api])

    # struct elements can be updated independently
    :ok =
      rpc.(Crdt, :for, [:struct_api])
      |> pipe.(Crdt, :at, [:user])
      |> pipe.(Crdt, :register, [:name, "bob"])
      |> pipe.(Crdt, :apply, [])

    {:ok, %{user: %SampleUser{name: "bob"}}} = rpc.(Crdt, :get, [:struct_api])
  end

  test "watch test", %{rpc: [rpc|_]} do
    pipe = fn x, m, f, a ->
      rpc.(m, f, [x | a])
    end
    :ok =
      rpc.(Crdt, :for, [:watch_test])
      |> pipe.(Crdt, :at, [:user])
      |> pipe.(Crdt, :register_struct, [%SampleUser{}])
      |> pipe.(Crdt, :apply, [])

    pid = self()
    {:ok, {watch, {:watch_test, %{user: %SampleUser{name: "alice", status: :online}}}}} = rpc.(Crdt, :watch, [:watch_test, pid])

    flush_watches(watch)

    :ok =
      rpc.(Crdt, :for, [:watch_test])
      |> pipe.(Crdt, :at, [:user])
      |> pipe.(Crdt, :register, [:status, :offline])
      |> pipe.(Crdt, :apply, [])

    :ok = receive do
      {^watch, {:watch_test, %{user: %SampleUser{name: "alice", status: :offline}}}} ->
        :ok
    after 100 ->
        raise "timeout"
    end
  end

  def flush_watches(watch) do
    receive do
      {^watch, _} ->
        flush_watches(watch)
    after 0 ->
        :ok
    end
  end
end
