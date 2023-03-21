defmodule Crdt.MixProject do
  use Mix.Project

  def project do
    [
      app: :crdt,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      default_task: "compile",
      aliases: [
        test: "test --no-start"
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Crdt.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:riak_core, git: "https://github.com/JesseStimpson/riak_core", branch: "experimental"},
      {:riak_dt, git: "https://github.com/basho/riak_dt.git", branch: "develop"},
      {:local_cluster, git: "https://github.com/whitfin/local-cluster.git", branch: "master", only: [:test]}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
