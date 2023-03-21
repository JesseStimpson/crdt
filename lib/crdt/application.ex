defmodule Crdt.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    :ok = :riak_core.register([{:vnode_module, Crdt.Vnode}])
    :ok = :riak_core_node_watcher.service_up(Crdt, self())

    children = [
      %{
        id: Crdt.Vnode_master,
        start: {:riak_core_vnode_master, :start_link, [Crdt.Vnode]},
        type: :worker
      }
      # Starts a worker by calling: Crdt.Worker.start_link(arg)
      # {Crdt.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Crdt.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
