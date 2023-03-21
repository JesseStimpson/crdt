defmodule Crdt.DefCrdt.Compiler do

  defguardp is_var(var)
  when is_tuple(var) and tuple_size(var) == 3 and is_atom(elem(var, 0)) and
  is_atom(elem(var, 2))

  defguardp is_underscore(var)
  when is_tuple(var) and tuple_size(var) == 3 and elem(var, 0) == :_ and
  is_atom(elem(var, 2))

  @doc false
  def __compile__(%Macro.Env{module: module, file: file, line: line}, exports) do
    {defcrdt_exports, []} =
      Enum.split_with(exports, fn {_fun_arity, meta} -> meta.type == :crdt end)

    defcrdts = compile_prepare_arities(defcrdt_exports)

    state = %{
      module: module,
      file: file,
      line: line,
      function: nil,
      defcrdts: defcrdts,
      rewrite_underscore?: false
    }

    quoted =
      Enum.map(defcrdt_exports, &compile_each_defcrdt(&1, state))

    {:__block__, [], quoted}
  end

  defp compile_prepare_arities(definitions) do
    for {{name, arity}, %{defaults: defaults}} <- definitions,
      arity <- (arity - map_size(defaults))..arity,
      into: MapSet.new(),
      do: {name, arity}
  end

  defp compile_each_defcrdt({{name, arity} = def, def_meta}, state) do
    %{defaults: defaults} = def_meta
    {{kind, _meta, args, ast}, state} = get_and_normalize_defcrdt(def, state)

    defcrdt_name = defcrdt_name(name)

    defcrdt_args =
      Enum.with_index(args, fn arg, i ->
        case defaults do
          %{^i => {meta, default}} -> {:\\, meta, [arg, default]}
          %{} -> arg
        end
      end)

    all_args = Macro.generate_arguments(arity, __MODULE__)
    Module.delete_definition(state.module, def)

    entrypoint =
      quote line: state.line do
        Kernel.unquote(kind)(unquote(name)(unquote_splicing(all_args))) do
          if Process.get(Crdt.DefCrdt.Compiler) do
            unquote(defcrdt_name)(unquote_splicing(all_args))
          else
            Crdt.DefCrdt.Compiler.__runtime__(
              &(unquote(Macro.var(defcrdt_name, __MODULE__)) / unquote(arity)),
              unquote(all_args)
            )
          end
        end
      end

    impl =
        quote line: state.line do
          Kernel.unquote(kind)(unquote(defcrdt_name)(unquote_splicing(defcrdt_args)), do: unquote(ast))
        end

    {strip_definition_context(entrypoint), impl}
  end

  # If the definition has a context, we don't warn when it goes unused,
  # so we remove the context as we want to keep the original semantics.
  defp strip_definition_context({kind, meta, [signature, block]}) do
    {kind, meta, [Macro.update_meta(signature, &Keyword.delete(&1, :context)), block]}
  end

  defp get_and_normalize_defcrdt({name, arity} = def, state) do
    {:v1, kind, meta, clauses} = Module.get_definition(state.module, def)
    state = %{state | function: def, line: meta[:line] || state.line, rewrite_underscore?: true}

    type_str = if kind == :def, do: "defcrdt", else: "defcrdtp"

    case clauses do
      [] ->
        compile_error!(meta, state, "cannot have #{type_str} #{name}/#{arity} without clauses")

      [{meta, args, [], ast}] ->
        {args, state} = normalize_args(args, meta, state)
        {ast, state} = normalize(ast, %{state | rewrite_underscore?: false})
        {{kind, meta, args, ast}, state}

      [_, _ | _] ->
        compile_error!(
          meta,
          state,
          "cannot compile #{type_str} #{name}/#{arity} with multiple clauses"
        )
    end
  end

  ## Normalize args

  defp normalize_args(args, meta, state) when is_list(args) do
    {args, state} = Enum.map_reduce(args, state, &normalize_arg(&1, meta, &2))
    assert_uniq_vars!(args, state)
    {args, state}
  end

  defp normalize_arg(var, _meta, state) when is_var(var) do
    if state.rewrite_underscore? and is_underscore(var) do
      {Macro.unique_var(:arg, state.module), state}
    else
      normalize(var, state)
    end
  end

  defp normalize_arg({:%, meta, [aliases, {:%{}, meta, args}]}, _meta, state) do
    {args, state} =
      Enum.map_reduce(args, state, fn {k, v}, acc ->
        {v, acc} = normalize_arg(v, meta, acc)
        {{k, v}, acc}
      end)

    {{:%, meta, [aliases, {:%{}, meta, args}]}, state}
  end

  defp normalize_arg({:%{}, meta, args}, _meta, state) do
    {args, state} =
      Enum.map_reduce(args, state, fn {k, v}, acc ->
        {v, acc} = normalize_arg(v, meta, acc)
        {{k, v}, acc}
      end)

    {{:%{}, meta, args}, state}
  end

  defp normalize_arg({op, meta, args}, _meta, state) when op in [:{}, :=] do
    {args, state} = Enum.map_reduce(args, state, &normalize_arg(&1, meta, &2))
    {{op, meta, args}, state}
  end

  defp normalize_arg({left, right}, meta, state) do
    {left, state} = normalize_arg(left, meta, state)
    {right, state} = normalize_arg(right, meta, state)
    {{:{}, meta, [left, right]}, state}
  end

  defp normalize_arg(expr, meta, state) do
    compile_error!(
      meta,
      state,
      "only variables, tuples, maps, and structs are allowed as patterns in defcrdt, got: #{Macro.to_string(expr)}"
    )
  end

  defp compile_error!(meta, state, description) do
    line = meta[:line] || state.line
    raise CompileError, line: line, file: state.file, description: description
  end

  defp defcrdt_name(name), do: :"__defcrdt:#{name}__"
end
