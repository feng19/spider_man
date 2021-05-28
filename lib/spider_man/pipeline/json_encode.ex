defmodule SpiderMan.Pipeline.JsonEncode do
  @moduledoc false
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(%{value: value}, encode_opts) do
    Jason.encode(value, encode_opts)
  end

  @impl true
  def prepare_for_start(encode_opts, options) do
    unless Code.ensure_loaded?(Jason) do
      raise "Please add Jason lib to your deps."
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
