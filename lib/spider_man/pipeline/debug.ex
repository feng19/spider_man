defmodule SpiderMan.Pipeline.Debug do
  require Logger
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(data, %{spider: spider, component: component, log_prefix: log_prefix}) do
    Logger.debug("#{log_prefix} #{component} get message: #{inspect(data)}", spider: spider)
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

    {%{spider: options[:spider], component: options[:component], log_prefix: log_prefix}, options}
  end
end
