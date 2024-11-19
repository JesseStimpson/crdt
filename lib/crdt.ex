defmodule Crdt do
  @moduledoc """
  Documentation for `Crdt`.
  """

  # Note: riak_core adds "_master" to the vnode module. That's why it looks weird
  @vnode_master Crdt.Vnode_master
  @node_service Crdt
  @bucket "crdt"
  @timeout 5000
  @replicas 8

  defstruct name: nil, key: [], op: nil

  def ring_status() do
    {:ok, ring} = :riak_core_ring_manager.get_my_ring()
    :riak_core_ring.pretty_print(ring, [:legend])
  end

  @doc """
  ## Examples

      iex> Crdt.for("owner_id")
      ...> |> Crdt.at("post_id")
      ...> |> Crdt.increment(:like)
      %Crdt{name: "owner_id",
        key: ["post_id"],
        op: {:update, [{:update, {:like, :riak_dt_emcntr}, {:increment, 1}}]}}

      iex> Crdt.for(:my_page)
      ...> |> Crdt.at(:a_user)
      ...> |> Crdt.register(:name, "alice")
      %Crdt{name: :my_page,
        key: [:a_user],
        op: {:update, [{:update, {:name, :riak_dt_lwwreg}, {:assign, "alice"}}]}}

      iex> Crdt.for(:my_calendar)
      ...> |> Crdt.at(:today)
      ...> |> Crdt.register_struct(%Date{year: 2023, month: 4, day: 5})
      [
        %Crdt{name: :my_calendar, key: [:today], op: {:update, [{:update, {:__struct__, :riak_dt_lwwreg}, {:assign, Date}}]}},
        %Crdt{name: :my_calendar, key: [:today], op: {:update, [{:update, {:calendar, :riak_dt_lwwreg}, {:assign, Calendar.ISO}}]}},
        %Crdt{name: :my_calendar, key: [:today], op: {:update, [{:update, {:day, :riak_dt_lwwreg}, {:assign, 5}}]}},
        %Crdt{name: :my_calendar, key: [:today], op: {:update, [{:update, {:month, :riak_dt_lwwreg}, {:assign, 4}}]}},
        %Crdt{name: :my_calendar, key: [:today], op: {:update, [{:update, {:year, :riak_dt_lwwreg}, {:assign, 2023}}]}}
      ]

  """
  def for(name) do
    %Crdt{name: name}
  end

  def at(crdt = %{key: key}, new_key) do
    %Crdt{crdt | key: key ++ [new_key]}
  end

  def increment(crdt = %Crdt{op: nil}, cntr_key, i \\ 1) do
    crdt_field = {cntr_key, :riak_dt_emcntr}
    increment_op = {:update, crdt_field, {:increment, i}}
    update_op = {:update, [increment_op]}
    %Crdt{crdt | op: update_op}
  end

  def decrement(crdt = %Crdt{op: nil}, cntr_key, i \\ 1) do
    crdt_field = {cntr_key, :riak_dt_emcntr}
    increment_op = {:update, crdt_field, {:decrement, i}}
    update_op = {:update, [increment_op]}
    %Crdt{crdt | op: update_op}
  end

  def register_struct(crdt = %Crdt{op: nil}, reg_struct) do
    reg_struct
    |> Map.to_list()
    |> Enum.map(fn {k, v} ->
      # Note: v can be a struct, but we choose not to recurse.
      # This way, the caller is in control of what embedded kvs
      # are tracked by crdt objects vs. just registers in the
      # higher level crdt
      Crdt.register(crdt, k, v)
    end)
  end

  def register(crdt = %Crdt{op: nil}, reg_key, reg_val) do
    crdt_field = {reg_key, :riak_dt_lwwreg}
    assign_op = {:update, crdt_field, {:assign, reg_val}}
    update_op = {:update, [assign_op]}
    %Crdt{crdt | op: update_op}
  end

  def insert(crdt = %Crdt{op: nil}, set_key, set_val) do
    crdt_field = {set_key, :riak_dt_orswot}
    add_op = {:update, crdt_field, {:add, set_val}}
    update_op = {:update, [add_op]}
    %Crdt{crdt | op: update_op}
  end

  def remove(crdt = %Crdt{op: nil}, set_key, set_val) do
    crdt_field = {set_key, :riak_dt_orswot}
    remove_op = {:update, crdt_field, {:remove, set_val}}
    update_op = {:update, [remove_op]}
    %Crdt{crdt | op: update_op}
  end

  def apply(crdt) when is_struct(crdt, Crdt) do
    apply([crdt])
  end

  def apply(crdts) when is_list(crdts) do
    crdts
    |> List.flatten()
    |> group_by_name()
    |> apply_named_groups()
    |> handle_apply_result()
  end

  defp group_by_name(crdts) do
    crdts
    |> Enum.reduce(
      %{},
      fn
        %Crdt{name: name, key: key, op: op}, grouped when not is_nil(op) ->
          keyed_ops = Map.get(grouped, name, [])
          Map.put(grouped, name, [{key, op} | keyed_ops])

        _, grouped ->
          grouped
      end
    )
  end

  defp apply_named_groups(grouped) do
    grouped
    |> Enum.map(fn {name, keyed_ops} ->
      keyed_ops = Enum.reverse(keyed_ops)
      IO.inspect({name, keyed_ops})
      {name, do_op(name, keyed_ops)}
    end)
    |> Enum.into(%{})
  end

  # If one named crdt, then just return the result
  # If multiple named crdts, then we return the whole map
  defp handle_apply_result(result) when map_size(result) == 1 do
    [{_, result}] = Map.to_list(result)
    result
  end
  defp handle_apply_result(result), do: result

  def do_test() do
    name = :jms
    key = [:a]

    increment = fn cntr_name ->
      crdt_field = {cntr_name, :riak_dt_emcntr}
      {:update, crdt_field, :increment}
    end

    update = {:update, [increment.(:c)]}
    keyed_update = {key, update}
    :ok = do_op(name, [keyed_update])
    name
  end

  def do_op(name, keyed_ops) do
    # Find the closest partition, perform the action there, get the crdt as a result, merge it with the others
    doc_idx = hash_name(name)
    preflist = :riak_core_apl.get_apl(doc_idx, @replicas, @node_service)

    case sync_command_first_success_wins(
           name,
           {:get_partition_for_name, name},
           preflist,
           @timeout
         ) do
      {:ok, partition_id} ->
        do_op1(partition_id, preflist, name, keyed_ops)

      {:error, {:not_found, partition_id}} ->
        do_op1(partition_id, preflist, name, keyed_ops)

      error ->
        error
    end
  end

  defp do_op1(partition_id, preflist, name, keyed_ops) do
    index_node = :lists.keyfind(partition_id, 1, preflist)
    losers = :proplists.delete(partition_id, preflist)

    case :riak_core_vnode_master.sync_spawn_command(
           index_node,
           {:do, name, keyed_ops},
           @vnode_master
         ) do
      {:ok, crdt_map} ->
        :riak_core_vnode_master.command(losers, {:merge, name, crdt_map}, :ignore, @vnode_master)
        :ok

      error ->
        error
    end
  end

  def put(name, value),
    do: sync_command_first_success_wins(name, {:put, name, value}, @replicas, @timeout)

  def get(name),
    do: sync_command_first_success_wins(name, {:get, name, :map}, @replicas, @timeout)

  def delete(name),
    do: sync_command_first_success_wins(name, {:delete, name}, @replicas, @timeout)

  def names(), do: coverage_to_list(coverage_command(:names))
  def values(), do: coverage_to_list(coverage_command(:values))

  def clear() do
    {:ok, %{list: []}} = coverage_to_list(coverage_command(:clear))
    :ok
  end

  def watch(name, pid \\ nil), do: Crdt.Watch.start(name, 10000, pid)

  def refresh_watch(name, receiver, ref) do
    doc_idx = hash_name(name)
    preflist = :riak_core_apl.get_apl(doc_idx, @replicas, @node_service)

    case sync_command_first_success_wins(
           name,
           {:get_partition_for_name, name},
           preflist,
           @timeout
         ) do
      {:ok, partition_id} ->
        index_node = :lists.keyfind(partition_id, 1, preflist)
        losers = :proplists.delete(partition_id, preflist)
        # We choose to watch first, then unwatch => duplicates, but no misses
        res =
          :riak_core_vnode_master.sync_spawn_command(
            index_node,
            {:watch, name, receiver, ref},
            @vnode_master
          )

        :riak_core_vnode_master.command(
          losers,
          {:unwatch, name, receiver},
          :ignore,
          @vnode_master
        )

        res

      error ->
        error
    end
  end

  def aae(name, timeout) do
    doc_idx = hash_name(name)
    preflist = :riak_core_apl.get_apl(doc_idx, @replicas, @node_service)

    try do
      case sync_command_all_successes(name, {:get, name, :raw}, preflist, timeout) do
        [first_value | values] ->
          merged_final =
            Enum.reduce(values, first_value, fn value, merged ->
              # Note: if Value =:= Merged riak_dt_map:merge/2 always returns the Merged,
              # even if the data passed in isn't a riak_dt_map at all
              try do
                :riak_dt_map.merge(value, merged)
              catch
                _ ->
                  merged
              end
            end)

          :riak_core_vnode_master.command(
            preflist,
            {:merge, name, merged_final},
            :ignore,
            @vnode_master
          )

          :ok

        [] ->
          :ok
      end
    catch
      :timeout ->
        {:error, :timeout}
    end
  end

  defp hash_name(name), do: :riak_core_util.chash_key({@bucket, :erlang.term_to_binary(name)})

  defp sync_command_all_successes(name, command, n, timeout) when is_integer(n) do
    doc_idx = hash_name(name)
    preflist = :riak_core_apl.get_apl(doc_idx, n, @node_service)
    sync_command_all_successes(name, command, preflist, timeout)
  end

  defp sync_command_all_successes(_name, command, preflist, timeout) do
    reqid = :erlang.phash2(make_ref())
    self = self()
    receiver_proxy = spawn_link(fn -> collect_responses(self, reqid, length(preflist), []) end)
    sender = {:raw, reqid, receiver_proxy}
    :riak_core_vnode_master.command(preflist, command, sender, @vnode_master)

    receive do
      {^reqid, reply} ->
        Enum.reverse(reply)
    after
      timeout ->
        Process.unlink(receiver_proxy)
        Process.exit(receiver_proxy, :kill)
        raise "timeout"
    end
  end

  defp sync_command_first_success_wins(name, command, n, timeout) when is_integer(n) do
    doc_idx = hash_name(name)
    preflist = :riak_core_apl.get_apl(doc_idx, n, @node_service)
    sync_command_first_success_wins(name, command, preflist, timeout)
  end

  defp sync_command_first_success_wins(_name, command, preflist, timeout)
       when is_list(preflist) do
    reqid = :erlang.phash2(make_ref())
    self = self()

    receiver_proxy =
      spawn_link(fn -> look_for_success(self, reqid, length(preflist), {:error, :no_response}) end)

    sender = {:raw, reqid, receiver_proxy}
    :riak_core_vnode_master.command(preflist, command, sender, @vnode_master)

    receive do
      {^reqid, reply} ->
        reply
    after
      timeout ->
        Process.unlink(receiver_proxy)
        Process.exit(receiver_proxy, :kill)
        raise "timeout"
    end
  end

  defp collect_responses(pid, reqid, 0, acc), do: send(pid, {reqid, acc})

  defp collect_responses(pid, reqid, n, acc) do
    receive do
      {^reqid, {:error, _}} ->
        collect_responses(pid, reqid, n - 1, acc)

      {^reqid, {:ok, resp}} ->
        collect_responses(pid, reqid, n - 1, [resp | acc])
    end
  end

  defp look_for_success(pid, reqid, 0, first_error), do: send(pid, {reqid, first_error})

  defp look_for_success(pid, reqid, n, first_error) do
    receive do
      {^reqid, :ok} ->
        send(pid, {reqid, :ok})

      {^reqid, {:ok, reply}} ->
        send(pid, {reqid, {:ok, reply}})

      {^reqid, error = {:error, _reason}} ->
        case first_error do
          {:error, :no_response} ->
            look_for_success(pid, reqid, n - 1, error)

          _ ->
            look_for_success(pid, reqid, n - 1, first_error)
        end
    end
  end

  defp coverage_command(command) do
    :riak_core_coverage_fold.run_link(
      fn partition, node, data, acc ->
        [{partition, node, data} | acc]
      end,
      [],
      {@vnode_master, @node_service, command},
      @timeout
    )
  end

  defp coverage_to_list({:ok, coverage_result}) do
    {:ok,
     Enum.reduce(
       coverage_result,
       fn
         {_partition, _node, []}, acc = %{n: t} ->
           %{acc | n: t + 1}

         {_partition, _node, l}, acc = %{list: list, from: c, n: t} ->
           %{acc | list: list ++ l, from: c + 1, n: t + 1}
       end
     )}
  end

  defp coverage_to_list(error), do: error
end
