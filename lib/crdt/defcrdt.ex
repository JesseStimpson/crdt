defmodule Crdt.DefCrdt do

  @compiler_key {Crdt.DefCrdt, :default_compiler}
  @app_key :default_defcrdt_options

  defmacro defcrdt(call, do: block) do
    define_defcrdt(:def, call, block, __CALLER__)
  end

  defp define_defcrdt(kind, call, block, env) do
    assert_no_guards!(kind, call, env)
    {name, args} = decompose_call!(kind, call, env)
    arity = length(args)

    defaults =
      for {{:\\, meta, [_, default]}, i} <- Enum.with_index(args),
      do: {i, {meta, Macro.escape(default)}},
      into: []

    quote do
      unquote(__MODULE__).__define__(
        __MODULE__,
        unquote(kind),
        unquote(name),
        unquote(arity),
        :crdt,
        %{unquote_splicing(defaults)}
      )
      unquote(kind)(unquote(call)) do
        use Crdt.DefCrdt.Kernel
        unquote(block)
      end
      Process.delete(Crdt.DefCrdt)
    end
  end

  defp decompose_call!(kind, {:when, _, [call, _guards]}, env),
    do: decompose_call!(kind, call, env)
  defp decompose_call!(_kind, {{:unquote, _, [name]}, _, args}, _env) do
    {name, args}
  end
  defp decompose_call!(kind, call, env) do
    case Macro.decompose_call(call) do
      {name, args} ->
        {name, args}
      :error ->
        compile_error!(
          env,
          "first argument of #{kind}crdt must be a call, got: #{Macro.to_string(call)}"
        )
    end
  end

  defp assert_no_guards!(kind, {:when, _, _}, env) do
    compile_error!(env, "guards are not supported by #{kind}crdt")
  end
  defp assert_no_guards!(_kind, _call, _env), do: :ok

  # Internal attributes
  @defcrdt_exports_key :__defcrdt_exports__

  @doc false
  def __define__(module, kind, name, arity, type, defaults) do
    exports =
      if exports = Module.get_attribute(module, @defcrdt_exports_key) do
        exports
      else
        Module.put_attribute(module, :before_compile, __MODULE__)
        %{}
      end

    current_export = %{
      type: type,
      kind: kind,
      defaults: defaults
    }

    exports = if type == :crdt do
      Process.put(Crdt.DefCrdt, true)
      Map.put(exports, {name, arity}, current_export)
    end

    Module.put_attribute(module, @defcrdt_exports_key, exports)
    :ok
  end

  defp compile_error!(env, description) do
    raise CompileError, line: env.line, file: env.file, description: description
  end

  @doc false
  defmacro __before_compile__(env) do
    defcrdt_exports = Module.get_attribute(env.module, @defcrdt_exports_key)
    Crdt.DefCrdt.Compiler.__compile__(env, defcrdt_exports)
  end
end
