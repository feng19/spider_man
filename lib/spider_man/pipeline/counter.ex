defmodule SpiderMan.Pipeline.Counter do
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(response, tid) do
    :ets.update_counter(tid, __MODULE__, {2, 1})
    response
  end

  @impl true
  def prepare_for_start(arg, options) do
    tid =
      case arg do
        :common -> options[:common_pipeline_tid]
        [scope: :common] -> options[:common_pipeline_tid]
        _ -> options[:pipeline_tid]
      end

    :ets.insert(tid, {__MODULE__, 0})
    {tid, options}
  end

  def get(tid), do: :ets.lookup_element(tid, __MODULE__, 2)
end
