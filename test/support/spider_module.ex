defmodule SpiderMan.Modules do
  def create_spider(module_name) do
    with {:module, spider, _binary, _term} <-
           Module.create(
             module_name,
             quote do
               use SpiderMan
               @impl true
               def settings do
                 [
                   downloader_options: [pipelines: [], requester: SpiderMan.Requester.JustReturn],
                   spider_options: [pipelines: []],
                   item_processor_options: [pipelines: [], batchers: []]
                 ]
               end

               @impl true
               def handle_response(_, _context), do: %{}
             end,
             Macro.Env.location(__ENV__)
           ) do
      {:ok, spider}
    end
  end

  def create_spiders(count) do
    Enum.map(1..count, fn n ->
      {:ok, spider} = create_spider(:"#{Spider}#{n}")
      spider
    end)
  end

  def setup_all do
    spider = get()
    {:ok, _} = SpiderMan.start(spider)
    SpiderMan.wait_until(spider)
    spider
  end

  def on_exit(spider) do
    SpiderMan.stop(spider)
    put(spider)
  end

  def start_agent(count) do
    spiders = create_spiders(count)
    {:ok, agent} = Agent.start_link(fn -> spiders end)
    :persistent_term.put(:spiders_agent, agent)
  end

  def get() do
    agent = :persistent_term.get(:spiders_agent)

    with nil <-
           Agent.get_and_update(agent, fn
             [spider | spiders] -> {spider, spiders}
             [] -> {nil, []}
           end) do
      Process.sleep(100)
      get()
    end
  end

  def put(spider) do
    agent = :persistent_term.get(:spiders_agent)
    Agent.update(agent, fn spiders -> [spider | spiders] end)
  end
end

defmodule Spider0 do
  use SpiderMan
  alias SpiderMan.{Requester.JustReturn, Response, Utils}
  alias SpiderMan.Pipeline.{SetCookie, DuplicateFilter}

  @impl true
  def settings do
    [
      downloader_options: [
        # context: %{debug: true},
        pipelines: [DuplicateFilter, SetCookie],
        requester:
          {JustReturn,
           [
             logging?: true,
             base_url: "https://www.example.com"
             # proxy: %{schema: :http, address: "127.0.0.1", port: 1087}
           ]}
      ],
      spider_options: [
        # context: %{debug: true},
        pipelines: [DuplicateFilter, SetCookie]
      ],
      item_processor_options: [
        # context: %{debug: true}
        # batchers: []
      ]
    ]
  end

  @impl true
  def handle_response(%Response{key: key, env: env}, context) do
    if parent = Map.get(context, :parent) do
      send(parent, {:handle_response, env})
    end

    case key do
      "/" ->
        items = Enum.map(1..10, &Utils.build_item(&1, &1))
        requests = Enum.map(1..10, &Utils.build_request("/#{&1}"))
        %{items: items, requests: requests}

      _ ->
        %{items: [], requests: []}
    end
  end
end
