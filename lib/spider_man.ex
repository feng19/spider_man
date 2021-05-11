defmodule SpiderMan do
  @moduledoc """
  Documentation for `SpiderMan`.

  ## Spider Life Cycle
    0. `Spider.settings()`
    1. `Spider.prepare_for_start(:pre, state)`
    2. `Spider.prepare_for_start_component(:downloader, state)`
    3. `Spider.prepare_for_start_component(:spider, state)`
    4. `Spider.prepare_for_start_component(:item_processor, state)`
    5. `Spider.prepare_for_start(:post, state)`
    6. `Spider.init(state)`
    6. `Spider.handle_response(response, context)`
    7. `Spider.prepare_for_stop_component(:downloader, state)`
    8. `Spider.prepare_for_stop_component(:spider, state)`
    9. `Spider.prepare_for_stop_component(:item_processor, state)`
    10. `Spider.prepare_for_stop(state)`
  """
  alias SpiderMan.{Engine, Configuration, Request, Response, Item}

  @type spider :: module | atom
  @type settings :: keyword
  @type status :: :running | :suspended
  @type request :: Request.t()
  @type requests :: [request]
  @type component :: :downloader | :spider | :item_processor
  @type component_stats :: [size: pos_integer, memory: pos_integer] | nil
  @type prepare_for_start_stage :: :pre | :post

  @components [:downloader, :spider, :item_processor]

  @callback handle_response(Response.t(), context :: map) :: %{
              optional(:requests) => [Request.t()],
              optional(:items) => [Item.t()]
            }
  @callback settings() :: settings
  @callback init(state) :: state when state: Engine.state()
  @callback prepare_for_start(prepare_for_start_stage, state) :: state when state: Engine.state()
  @callback prepare_for_stop(Engine.state()) :: :ok
  @callback prepare_for_start_component(component, options) :: options when options: keyword
  @callback prepare_for_stop_component(component, options :: keyword) :: :ok
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
          build_item: 2,
          build_item: 3,
          build_item: 4
        ]

      @behaviour SpiderMan
    end
  end

  @doc """
  start a spider

  ## Settings
  #{Configuration.configuration_docs()}
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

  @doc "insert a request to spider"
  @spec insert_request(spider, request) :: true | nil
  def insert_request(spider, request) when is_struct(request, Request),
    do: insert_requests(spider, [request])

  @doc "insert multiple requests to spider"
  @spec insert_requests(spider, requests) :: true | nil
  def insert_requests(spider, requests) do
    if tid = :persistent_term.get({spider, :downloader_tid}, nil) do
      objects = Enum.map(requests, &{&1.key, &1})
      :ets.insert(tid, objects)
    end
  end

  @doc "fetch spider's statistics"
  @spec stats(spider) :: [
          status: status,
          downloader: component_stats,
          spider: component_stats,
          item_processor: component_stats
        ]
  def stats(spider) do
    components = Enum.map(@components, &{&1, stats(spider, &1)})
    [{:status, Engine.status(spider)} | components]
  end

  @doc "fetch component's statistics"
  @spec stats(spider, component) :: component_stats
  def stats(spider, component) do
    if tid = :persistent_term.get({spider, :"#{component}_tid"}, nil) do
      tid |> :ets.info() |> Keyword.take([:size, :memory])
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
  def periodic_measurements() do
    Enum.each(list_spiders(), &telemetry_execute(&1))
  catch
    _, _ -> :ok
  end

  @doc false
  def telemetry_execute(spider) do
    name = inspect(spider)

    Enum.each(@components, fn component ->
      measurements = stats(spider, component) |> Map.new()
      :telemetry.execute([:spider_man, :ets], measurements, %{name: name, component: component})
    end)
  catch
    _, _ -> :ok
  end
end
