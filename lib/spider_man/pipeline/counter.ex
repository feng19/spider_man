defmodule SpiderMan.Pipeline.Counter do
  @moduledoc """
  msg counter for component

  ## Usage
  ```elixir
  settings = [
    ...
    *_options: [
      pipelines: [#{inspect(__MODULE__)} | {#{inspect(__MODULE__)}, scope}]
    ]
  ]
  ```

  Support for all component: `downloader` | `spider` | `item_processor`.

  ### Scope
  * `:common` | `[scope: :common]`: save counter to `common_pipeline_tid`.
  * `:pipeline` | `[scope: :pipeline]`:  save counter to `pipeline_tid`.

  `common_pipeline_tid` use by all component, `pipeline_tid` only use by one component.
  """
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
