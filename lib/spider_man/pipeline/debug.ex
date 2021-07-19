defmodule SpiderMan.Pipeline.Debug do
  @moduledoc """
  use for debug msg by component

  ## Usage
  ```elixir
  settings = [
    ...
    *_options: [
      pipelines: [#{inspect(__MODULE__)} | {#{inspect(__MODULE__)}, log_prefix :: String.t()}]
    ]
  ]
  ```

  Support for all component: `downloader` | `spider` | `item_processor`.
  """
  require Logger
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(data, %{component: component, log_prefix: log_prefix}) do
    Logger.debug("#{log_prefix} #{component} get message: #{inspect(data)}")
    data
  end

  @impl true
  def prepare_for_start(log_prefix, options) do
    log_prefix =
      if is_binary(log_prefix) do
        log_prefix
      else
        ""
      end

    {%{component: options[:component], log_prefix: log_prefix}, options}
  end
end
