defmodule SpiderMan.Pipeline.DuplicateFilter do
  @moduledoc false
  require Logger
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(event, tid) do
    if :ets.insert_new(tid, {{__MODULE__, event.key}, nil}) do
      event
    else
      Logger.debug(">>> Remove event: #{inspect(event)} by #{inspect(__MODULE__)}.")
      :skiped
    end
  end

  @impl true
  def prepare_for_start(:all_components, options) do
    {options[:common_pipeline_tid], options}
  end

  def prepare_for_start(_arg, options) do
    {options[:pipeline_tid], options}
  end
end
