defmodule SpiderMan.Pipeline.DuplicateFilter do
  @moduledoc """
  filter msg while duplicate key

  ## Usage

      settings = [
        *_options: [
          pipelines: [#{inspect(__MODULE__)} | {#{inspect(__MODULE__)}, scope}]
        ],
        ...
      ]

  Support for all component: `downloader` | `spider` | `item_processor`.

  ### Scope
  * `:common` | `[scope: :common]`: save key to `common_pipeline_tid`.
  * `:pipeline` | `[scope: :pipeline]`:  save key to `pipeline_tid`.

  `common_pipeline_tid` use by all component, `pipeline_tid` only use by one component.
  """
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
  def prepare_for_start(arg, options) do
    tid =
      case arg do
        :common -> options[:common_pipeline_tid]
        [scope: :common] -> options[:common_pipeline_tid]
        _ -> options[:pipeline_tid]
      end

    {tid, options}
  end
end
