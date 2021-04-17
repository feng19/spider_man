defmodule SpiderMan.Pipeline.Counter do
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(response, tid) do
    :ets.update_counter(tid, __MODULE__, {2, 1})
    response
  end

  @impl true
  def prepare_for_start(:common, options) do
    tid = options[:common_pipeline_tid]
    :ets.insert(tid, {__MODULE__, 0})
    {tid, options}
  end

  def prepare_for_start(_arg, options) do
    tid = options[:pipeline_tid]
    :ets.insert(tid, {__MODULE__, 0})
    {tid, options}
  end
end
