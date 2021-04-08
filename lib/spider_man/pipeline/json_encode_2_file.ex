defmodule SpiderMan.Pipeline.JsonEncode2File do
  @moduledoc false
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(%{key: key, value: value}, dir) do
    file_name =
      if is_binary(dir) do
        Path.join([dir, key])
      else
        key
      end

    json = Jason.encode!(value)
    File.write!(file_name, json)
  end

  @impl true
  def prepare_for_start(arg, options) do
    if Code.ensure_loaded?(Jason) do
      raise "Please add Jason to your deps."
    end

    {arg, options}
  end
end
