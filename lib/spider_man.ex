defmodule SpiderMan do
  @moduledoc """
  SpiderMan, a fast high-level web crawling & scraping framework for Elixir.

  ## Components
  Each Spider had 3 components, each component has theirs work:

  * [Downloader](SpiderMan.Component.Downloader.html): Download request.
  * [Spider](SpiderMan.Component.Spider.html): Analyze web pages.
  * [ItemProcessor](SpiderMan.Component.ItemProcessor.html): Store items.

  Message flow: `Downloader` -> `Spider` -> `ItemProcessor`.

  ## Spider Life Cycle
    0. `Spider.settings()`
    1. Prepare For Start Stage
      1. `Spider.prepare_for_start(:pre, state)`
      2. `Spider.prepare_for_start_component(:downloader, state)`
      3. `Spider.prepare_for_start_component(:spider, state)`
      4. `Spider.prepare_for_start_component(:item_processor, state)`
      5. `Spider.prepare_for_start(:post, state)`
    2. `Spider.init(state)`
    3. `Spider.handle_response(response, context)`
    4. Prepare For Stop Stage
      1. `Spider.prepare_for_stop_component(:downloader, state)`
      2. `Spider.prepare_for_stop_component(:spider, state)`
      3. `Spider.prepare_for_stop_component(:item_processor, state)`
      4. `Spider.prepare_for_stop(state)`
  """
  alias SpiderMan.{Configuration, Engine, Item, Request, Response}

  @type spider :: module | atom

  @typedoc """
  #{Configuration.configuration_docs()}
  """
  @type settings :: keyword
  @type status :: :running | :suspended
  @type request :: Request.t()
  @type requests :: [request]
  @type component :: :downloader | :spider | :item_processor
  @type ets_stats :: [size: pos_integer, memory: pos_integer] | nil
  @type prepare_for_start_stage :: :pre | :post

  @callback handle_response(Response.t(), context :: map) :: %{
              optional(:requests) => [Request.t()],
              optional(:items) => [Item.t()]
            }
  @callback settings() :: settings
  @callback init(state) :: state when state: Engine.state()
  @callback prepare_for_start(prepare_for_start_stage, state) :: state when state: Engine.state()
  @callback prepare_for_stop(Engine.state()) :: :ok
  @callback prepare_for_start_component(component, options | false) :: options
            when options: keyword
  @callback prepare_for_stop_component(component, options :: keyword | false) :: :ok
  @optional_callbacks settings: 0,
                      init: 1,
                      prepare_for_start: 2,
                      prepare_for_stop: 1,
                      prepare_for_start_component: 2,
                      prepare_for_stop_component: 2

  @doc false
  defmacro __using__(_opts \\ []) do
    quote do
      import SpiderMan.Utils,
        only: [
          build_request: 1,
          build_request: 2,
          build_request: 3,
          build_requests: 1,
          build_requests: 2,
          build_requests: 3,
          build_item: 2,
          build_item: 3,
          build_item: 4,
          set_key: 2,
          set_flag: 2
        ]

      import SpiderMan, only: [insert_request: 2, insert_requests: 2]

      @behaviour SpiderMan
    end
  end

  @doc """
  start a spider
  """
  @spec start(spider, settings) :: Supervisor.on_start_child()
  defdelegate start(spider, settings \\ []), to: SpiderMan.Application, as: :start_child

  @doc "stop a spider"
  @spec stop(spider) :: :ok | {:error, error} when error: :not_found | :running | :restarting
  defdelegate stop(spider), to: SpiderMan.Application, as: :stop_child

  @doc "fetch spider's status"
  @spec status(spider) :: status
  defdelegate status(spider), to: Engine

  @doc "fetch spider's state"
  @spec get_state(spider) :: Engine.state()
  defdelegate get_state(spider), to: Engine

  @doc "suspend a spider"
  @spec suspend(spider, timeout) :: :ok
  defdelegate suspend(spider, timeout \\ :infinity), to: Engine

  @doc "continue a spider"
  @spec continue(spider, timeout) :: :ok
  defdelegate continue(spider, timeout \\ :infinity), to: Engine

  @doc "retry failed events for a spider"
  @spec retry_failed(spider, max_retries :: integer, timeout) :: {:ok, count :: integer}
  defdelegate retry_failed(spider, max_retries \\ 3, timeout \\ :infinity), to: Engine

  @doc "insert a request to spider"
  @spec insert_request(spider, request) :: true | nil
  def insert_request(spider, request) when is_struct(request, Request),
    do: insert_requests(spider, [request])

  @doc "insert multiple requests to spider"
  @spec insert_requests(spider, requests) :: true | nil
  def insert_requests(spider, requests) do
    if info = :persistent_term.get(spider, nil) do
      objects = Enum.map(requests, &{&1.key, &1})

      :telemetry.execute(
        [:spider_man, :downloader, :start],
        %{count: length(objects)},
        %{name: inspect(spider)}
      )

      :ets.insert(info.downloader_tid, objects)
    end
  end

  @doc "fetch spider's statistics"
  @spec stats(spider) :: [
          status: status,
          common_pipeline_tid: ets_stats,
          downloader_tid: ets_stats,
          failed_tid: ets_stats,
          spider_tid: ets_stats,
          item_processor_tid: ets_stats
        ]
  def stats(spider) do
    components =
      :persistent_term.get(spider)
      |> Enum.map(fn {key, tid} ->
        {key,
         tid
         |> :ets.info()
         |> Keyword.take([:size, :memory])}
      end)

    [{:status, Engine.status(spider)} | components]
  end

  @doc "fetch spider's statistics of all ets"
  @spec ets_stats(spider) :: [
          common_pipeline_tid: ets_stats,
          downloader_tid: ets_stats,
          failed_tid: ets_stats,
          spider_tid: ets_stats,
          item_processor_tid: ets_stats
        ]
  def ets_stats(spider) do
    :persistent_term.get(spider)
    |> Enum.map(fn {key, tid} ->
      {key,
       tid
       |> :ets.info()
       |> Keyword.take([:size, :memory])}
    end)
  end

  @spec components :: [component]
  def components, do: [:downloader, :spider, :item_processor]

  @doc "fetch component's statistics"
  @spec stats(spider, component) :: ets_stats
  def stats(spider, component) do
    if info = :persistent_term.get(spider, nil) do
      info[:"#{component}_tid"] |> :ets.info() |> Keyword.take([:size, :memory])
    end
  end

  @spec run_until_zero(spider, settings, check_interval :: integer) :: millisecond :: integer
  def run_until_zero(spider, settings \\ [], check_interval \\ 1500) do
    run_until(spider, settings, fn ->
      ets_list =
        :persistent_term.get(spider)
        |> Map.take([:downloader_tid, :failed_tid, :spider_tid])

      fun = fn {_, tid} -> :ets.info(tid, :size) == 0 end

      if Enum.all?(ets_list, fun) do
        Process.sleep(check_interval)

        if Enum.all?(ets_list, fun) do
          :stop
        else
          check_interval
        end
      else
        check_interval
      end
    end)
  end

  @spec run_until(spider, settings, fun) :: millisecond :: integer
  def run_until(spider, settings \\ [], fun) when is_function(fun, 0) do
    t1 = System.system_time(:millisecond)
    {:ok, _} = start(spider, settings)
    Process.sleep(1500)
    _run_until(fun)
    :ok = stop(spider)
    System.system_time(:millisecond) - t1
  end

  defp _run_until(fun) do
    case fun.() do
      :stop ->
        :stop

      sleep_time when is_integer(sleep_time) ->
        Process.sleep(sleep_time)
        _run_until(fun)

      _ ->
        Process.sleep(100)
        _run_until(fun)
    end
  end

  @doc "list spiders where already started"
  @spec list_spiders :: [spider]
  def list_spiders do
    SpiderMan.Supervisor
    |> Supervisor.which_children()
    |> Enum.map(&elem(&1, 0))
  end

  @doc false
  def periodic_measurements do
    Enum.each(list_spiders(), &telemetry_execute(&1))
  catch
    _, _ -> :ok
  end

  @doc false
  def telemetry_execute(spider) do
    name = inspect(spider)

    spider
    |> ets_stats()
    |> Enum.each(fn {tid, measurements} ->
      :telemetry.execute([:spider_man, :ets], Map.new(measurements), %{name: name, tid: tid})
    end)
  catch
    _, _ -> :ok
  end
end
