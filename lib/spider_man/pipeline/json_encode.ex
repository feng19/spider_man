defmodule SpiderMan.Pipeline.JsonEncode do
  @moduledoc """
  Encode item.value to json for ItemProcessor component

  ## Usage
  ```elixir
  settings = [
    ...
    item_processor_options: [
      pipelines: [#{inspect(__MODULE__)} | {#{inspect(__MODULE__)}, encode_opts}]
    ]
  ]
  ```
  """
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(%{value: value}, encode_opts) do
    Jason.encode(value, encode_opts)
  end

  @impl true
  def prepare_for_start(encode_opts, options) do
    unless Code.ensure_loaded?(Jason) do
      raise "Please add :jason lib to your deps."
    end

    encode_opts =
      case encode_opts do
        [encode_opts: encode_opts] -> encode_opts
        opts when is_list(opts) -> opts
        _ -> []
      end

    {encode_opts, options}
  end
end
