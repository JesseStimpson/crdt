defmodule Crdt.Vnode do
  @behaviour :riak_core_vnode
  defstruct partition: 0, data: %{}, watches: %{}, watch_monitors: %{}
  alias Crdt.Vnode

  require Logger

  @impl true
  def init([partition]) do
    {:ok, %Vnode{partition: partition}}
  end

  @impl true
  def handle_command(
        {:get_partition_for_name, name},
        _sender,
        state = %Vnode{partition: partition, data: data}
      ) do
    case Map.has_key?(data, name) do
      true ->
        {:reply, {:ok, partition}, state}

      false ->
        {:reply, {:error, {:not_found, partition}}, state}
    end
  end

  def handle_command({:watch, name, receiver, ref}, _sender, state = %Vnode{data: data}) do
    case {add_watch(name, receiver, ref, state), Map.fetch(data, name)} do
      {{true, state}, {:ok, value}} ->
        {:reply, {:ok, crdt_map_to_value(value)}, handle_watches(name, value, state)}

      {{false, state}, {:ok, value}} ->
        {:reply, {:ok, crdt_map_to_value(value)}, state}

      {{_, state}, :error} ->
        {:reply, {:ok, :not_found}, state}
    end
  end

  def handle_command({:unwatch, name, receiver}, _sender, state = %Vnode{}) do
    {:reply, :ok, remove_watch(name, receiver, state)}
  end

  def handle_command(
        {:do, name, keyed_ops},
        _sender,
        state = %Vnode{partition: partition, data: data}
      ) do
    update = build_update_chains_from_keyed_ops(keyed_ops)

    crdt_map =
      case Map.fetch(data, name) do
        {:ok, m} -> m
        :error -> :riak_dt_map.new()
      end

    log("DO #{inspect(update)} #{pname(partition)}", state)

    {:ok, crdt_map} = :riak_dt_map.update(update, partition, crdt_map)
    data = Map.put(data, name, crdt_map)
    {:reply, {:ok, crdt_map}, handle_watches(name, crdt_map, %Vnode{state | data: data})}
  end

  def handle_command({:put, name, value}, _sender, state = %Vnode{data: data}) do
    case Map.fetch(data, name) do
      {:ok, ^value} ->
        {:reply, :ok, state}

      _ ->
        log("PUT #{inspect(name)}:#{inspect(value)}", state)
        data = Map.put(data, name, value)
        {:reply, :ok, handle_watches(name, value, %Vnode{state | data: data})}
    end
  end

  def handle_command({:merge, name, value}, _sender, state = %Vnode{data: data}) do
    case Map.fetch(data, name) do
      {:ok, ^value} ->
        {:reply, :ok, state}

      {:ok, other_value} ->
        value =
          try do
            :riak_dt_map.merge(value, other_value)
          catch
            _ -> value
          end

        data = Map.put(data, name, value)
        {:reply, :ok, handle_watches(name, value, %Vnode{state | data: data})}

      _ ->
        data = Map.put(data, name, value)
        {:reply, :ok, handle_watches(name, value, %Vnode{state | data: data})}
    end
  end

  def handle_command({:get, name, structure}, _sender, state = %Vnode{data: data}) do
    case Map.fetch(data, name) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, value} ->
        case structure do
          :raw ->
            {:reply, {:ok, value}, state}

          :map ->
            {:reply, {:ok, crdt_map_to_value(value)}, state}
        end
    end
  end

  def handle_command({:delete, name}, _sender, state = %Vnode{data: data}) do
    log("DELETE #{inspect(name)}", state)
    value = Map.get(data, name, :not_found)
    data = Map.delete(data, name)
    state = remove_all_watches(name, state)
    {:reply, {:ok, crdt_map_to_value(value)}, %Vnode{state | data: data}}
  end

  def handle_command(message, _sender, state = %Vnode{}) do
    log("UNHANDLED COMMAND #{inspect(message)}", state)
    {:noreply, state}
  end

  @impl true
  def handle_info(
        {:DOWN, mref, :process, pid, _reason},
        state = %Vnode{watch_monitors: watch_monitors}
      ) do
    case Map.fetch(watch_monitors, mref) do
      {:ok, {name, ^pid}} ->
        {:ok, remove_watch(name, pid, state)}

      # :error | {:ok, {name, different_pid}}
      _ ->
        {:ok, state}
    end
  end

  def handle_info(_info, state = %Vnode{}) do
    {:noreply, state}
  end

  @impl true
  # TODO: Finish implementing from rc_example
  def handle_handoff_command(message, sender, state) do
    log("HANDOFF COMMAND #{inspect(message)}", state)
    {:reply, _result, state} = handle_command(message, sender, state)
    {:forward, state}
  end

  @impl true
  def handle_handoff_data(bin_data, state) do
    log("HANDOFF DATA #{:erlang.size(bin_data)}", state)
    {name, value} = :erlang.binary_to_term(bin_data)
    handle_command({:merge, name, value}, :ignore, state)
  end

  @impl true
  def encode_handoff_item(name, value) do
    :erlang.term_to_binary({name, value})
  end

  @impl true
  def handle_coverage(:names, _key_spaces, {_, reqid, _}, state = %Vnode{data: data}) do
    names = Map.keys(data)
    {:reply, {reqid, names}, state}
  end

  def handle_coverage(:values, _key_spaces, {_, reqid, _}, state = %Vnode{data: data}) do
    values = Map.values(data)
    {:reply, {reqid, values}, state}
  end

  def handle_coverage(:clear, _key_spaces, {_, reqid, _}, state = %Vnode{}) do
    {:reply, {reqid, []}, %Vnode{state | data: %{}}}
  end

  defp handle_watches(name, value, state = %Vnode{watches: watches}) do
    named_watches = Map.get(watches, name, [])

    for {pid, {ref, _mref}} <- named_watches,
        do: send(pid, {ref, {name, crdt_map_to_value(value)}})

    state
  end

  defp add_watch(
         name,
         receiver,
         ref,
         state = %Vnode{watches: watches, watch_monitors: watch_monitors}
       ) do
    named_watches = Map.get(watches, name, [])

    case :proplists.get_value(receiver, named_watches) do
      {^ref, _mref} ->
        {false, state}

      {_, mref} ->
        named_watches = :lists.keystore(receiver, 1, named_watches, {receiver, {ref, mref}})
        {true, %Vnode{state | watches: Map.put(watches, name, named_watches)}}

      :undefined ->
        mref = :erlang.monitor(:process, receiver)
        named_watches = [{receiver, {ref, mref}} | named_watches]
        watch_monitors = Map.put(watch_monitors, mref, {name, receiver})

        {true,
         %Vnode{
           state
           | watches: Map.put(watches, name, named_watches),
             watch_monitors: watch_monitors
         }}
    end
  end

  defp remove_watch(
         name,
         receiver,
         state = %Vnode{watches: watches, watch_monitors: watch_monitors}
       ) do
    named_watches = Map.get(watches, name, [])

    case :proplists.get_value(receiver, named_watches) do
      {_ref, mref} ->
        :erlang.demonitor(mref)
        watch_monitors = Map.delete(watch_monitors, mref)

        case :proplists.delete(receiver, named_watches) do
          [] ->
            %Vnode{state | watches: Map.delete(watches, name), watch_monitors: watch_monitors}

          named_watches ->
            %Vnode{
              state
              | watches: Map.put(watches, name, named_watches),
                watch_monitors: watch_monitors
            }
        end

      :undefined ->
        state
    end
  end

  defp remove_all_watches(name, state = %Vnode{watches: watches, watch_monitors: watch_monitors}) do
    named_watches = Map.get(watches, name, [])

    old_mrefs =
      Enum.map(
        named_watches,
        fn {_receiver, {_ref, mref}} ->
          :erlang.demonitor(mref)
          mref
        end
      )

    %Vnode{
      state
      | watches: Map.delete(watches, name),
        watch_monitors: Map.drop(watch_monitors, old_mrefs)
    }
  end

  defp build_update_chains_from_keyed_ops(keyed_ops) do
    chains = for {key, op} <- keyed_ops, do: build_update_chain_from_key(key, op)
    top_level_ops = Enum.reduce(chains, [], fn {:update, tlo}, acc -> acc ++ tlo end)
    {:update, top_level_ops}
  end

#  # Key is a list of atoms that name nested maps
#  defp build_update_chain_from_key(key) do
#    # __accessed__ is a special key in every leaf map that counts the number of accesses. The riak
#    # API cannot make an empyy leaf map, so we just throw a counter in there.
#    final = {:update, [{:update, {:__accessed__, :riak_dt_emcntr}, :increment}]}
#    build_update_chain_from_key(key, final)
#  end

  # final is the list of operations to execute on the final map in the key (leaf)
  defp build_update_chain_from_key([], final) do
    final
  end

  defp build_update_chain_from_key([h | t], final) do
    {:update, [{:update, {h, :riak_dt_map}, build_update_chain_from_key(t, final)}]}
  end

  defp crdt_map_to_value(crdt_map) do
    field_list = :riak_dt_map.value(crdt_map)
    crdt_fields_to_value(:riak_dt_map, field_list)
  end

  defp crdt_fields_to_value(:riak_dt_map, field_list) when is_list(field_list) do
    for(
      {{field_name, field_type}, field_value} <- field_list,
      do: {field_name, crdt_fields_to_value(field_type, field_value)}
    )
    |> Enum.into(%{})
  end

  defp crdt_fields_to_value(:riak_dt_emcntr, i) when is_integer(i) do
    i
  end

  defp log(string, state) do
    log(string, [], state)
  end

  defp log(string, metadata, %Vnode{partition: partition}) do
    Logger.debug("[#{pname(partition)}] " <> string, metadata)
    :ok
  end

  defp pname(partition) do
    :erlang.iolist_to_binary(:io_lib.format("~.36B", [partition]))
  end
end
