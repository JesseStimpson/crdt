defmodule Crdt.Watch do
  use GenServer
  alias Crdt.Watch

  defstruct [:name, :pid, :ref, :tick]

  def start(name, tick) do
    pid = self()
    {:ok, watch} = GenServer.start_link(Watch, %Watch{name: name, pid: pid, tick: tick})

    case GenServer.call(watch, :start, 5000) do
      {:ok, result} ->
        {:ok, {watch, {name, result}}}

      error ->
        error
    end
  end

  @impl true
  def init(state), do: {:ok, %Watch{state | ref: self()}}

  @impl true
  def handle_call(:start, _from, state) do
    case do_watch(state) do
      error = {:error, _reason} ->
        {:stop, :normal, error, state}

      {:ok, result, state} ->
        {:reply, {:ok, result}, state}
    end
  end

  @impl true
  def handle_info(:watch, state) do
    # TODO: do something if the name is deleted
    {:ok, _result, state} = do_watch(state)
    {:noreply, state}
  end

  defp do_watch(state = %Watch{name: name, pid: pid, ref: ref, tick: tick}) do
    _ = Crdt.aae(name, tick)

    case Crdt.refresh_watch(name, pid, ref) do
      error = {:error, _reason} ->
        error

      {:ok, result} ->
        :erlang.send_after(tick, self(), :watch)
        {:ok, result, state}
    end
  end
end
