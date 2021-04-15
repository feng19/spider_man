defmodule SpiderMan do
  @moduledoc """
  Documentation for `SpiderMan`.
  """
  alias SpiderMan.{Engine, Storage, Pipeline.DuplicateFilter}

  @components [:downloader, :spider, :item_processor]

  defmodule Request do
    @moduledoc false
    @enforce_keys [:key, :url]
    defstruct [:key, :url, options: [], retries: 0]
    @type t :: %__MODULE__{key: term, url: binary, options: keyword, retries: integer}
  end

  defmodule Response do
    @moduledoc false
    @enforce_keys [:key, :env]
    defstruct [:key, :env, options: [], retries: 0]
    @type t :: %__MODULE__{key: term, env: Tesla.Env.t(), options: keyword, retries: integer}
  end

  defmodule Item do
    @moduledoc false
    @enforce_keys [:key, :value]
    defstruct [:key, :value, options: [], retries: 0]
    @type t :: %__MODULE__{key: term, value: term, options: keyword, retries: integer}
  end

  @type component :: :downloader | :spider | :item_processor
  @type settings :: keyword
  @type prepare_for_start_stage :: :pre | :post

  @callback handle_response(Response.t(), context :: map) :: %{
              optional(:requests) => [Request.t()],
              optional(:items) => [Item.t()]
            }
  @callback settings() :: settings
  @callback prepare_for_start(prepare_for_start_stage, state) :: state when state: Engine.state()
  @callback prepare_for_stop(Engine.state()) :: :ok
  @callback prepare_for_start_component(component, options) :: options when options: keyword
  @callback prepare_for_stop_component(component, options :: keyword) :: :ok
  @optional_callbacks settings: 0,
                      prepare_for_start: 2,
                      prepare_for_stop: 1,
                      prepare_for_start_component: 2,
                      prepare_for_stop_component: 2

  @default_settings [
    downloader_options: [
      pipelines: [DuplicateFilter],
      processor: [max_demand: 1],
      rate_limiting: [allowed_messages: 10, interval: 1000],
      context: %{}
    ],
    spider_options: [
      pipelines: [],
      processor: [max_demand: 1],
      context: %{}
    ],
    item_processor_options: [
      pipelines: [DuplicateFilter],
      storage: Storage.Log,
      context: %{},
      batchers: [
        default: [
          concurrency: 1,
          batch_size: 50,
          batch_timeout: 1000
        ]
      ]
    ]
  ]

  @doc false
  defmacro __using__(_opts \\ []) do
    quote do
      use Supervisor

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

      def start_link(options) when is_list(options) do
        spider = Keyword.fetch!(options, :spider)
        spider_module = Keyword.fetch!(options, :spider_module)
        Supervisor.start_link(spider_module, options, name: spider)
      end

      @impl Supervisor
      def init(options) do
        spider = Keyword.fetch!(options, :spider)

        children = [
          {SpiderMan.Component.Supervisor, spider},
          {SpiderMan.Engine, options}
        ]

        Supervisor.init(children, strategy: :one_for_all)
      end

      defoverridable start_link: 1, init: 1
    end
  end

  def default_settings, do: @default_settings

  defdelegate start(spider, settings \\ []), to: SpiderMan.Application, as: :start_child
  defdelegate stop(spider), to: SpiderMan.Application, as: :stop_child
  defdelegate status(spider), to: SpiderMan.Engine
  defdelegate get_state(spider), to: SpiderMan.Engine
  defdelegate suspend(spider, timeout \\ :infinity), to: SpiderMan.Engine
  defdelegate continue(spider, timeout \\ :infinity), to: SpiderMan.Engine

  def ensure_started(spider, settings \\ []) do
    with {:ok, _} = return <- start(spider, settings) do
      wait_until(spider)
      return
    end
  end

  def insert_requests(spider, requests) do
    if tid = :persistent_term.get({spider, :downloader_tid}, nil) do
      objects = Enum.map(requests, &{&1.key, &1})
      :ets.insert(tid, objects)
    end
  end

  def stats(spider) do
    components = Enum.map(@components, &{&1, stats(spider, &1)})
    [{:status, Engine.status(spider)} | components]
  end

  def stats(spider, component) do
    if tid = :persistent_term.get({spider, :"#{component}_tid"}, nil) do
      tid |> :ets.info() |> Keyword.take([:size, :memory])
    end
  end

  def list_spiders do
    SpiderMan.Supervisor
    |> Supervisor.which_children()
    |> Enum.map(&elem(&1, 0))
  end

  def periodic_measurements() do
    Enum.each(list_spiders(), &telemetry_execute(&1))
  catch
    _, _ -> :ok
  end

  def telemetry_execute(spider) do
    name = inspect(spider)

    Enum.each(@components, fn component ->
      measurements = stats(spider, component) |> Map.new()
      :telemetry.execute([:spider_man, :ets], measurements, %{name: name, component: component})
    end)
  catch
    _, _ -> :ok
  end

  def wait_until(spider, status \\ :running) do
    if Engine.status(spider) != status do
      Process.sleep(100)
      wait_until(spider, status)
    end
  end
end
