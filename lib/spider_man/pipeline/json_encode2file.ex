defmodule SpiderMan.Pipeline.JsonEncode2File do
  @moduledoc """
  Encode item.value to json and save to files for ItemProcessor component

  ## Usage
  ```elixir
  settings = [
    ...
    item_processor_options: [
      pipelines: [#{inspect(__MODULE__)} | {#{inspect(__MODULE__)}, dir | [dir: dir]}]
    ]
  ]
  ```

  The file name is equal to item.key.
  If didn't set dir for this pipeline, the default is current dir.
  """
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
    unless Code.ensure_loaded?(Jason) do
      raise "Please add :jason lib to your deps."
    end

    dir =
      case arg do
        [dir: dir] -> dir
        _ -> arg
      end

    {dir, options}
  end
end
