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
                   item_processor_options: [
                     pipelines: [],
                     storage: SpiderMan.Storage.Log
                   ]
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
end

SpiderMan.Modules.create_spider(SpiderManTest)
SpiderMan.Modules.create_spider(EngineTest)

defmodule Spider0 do
  use SpiderMan
  alias SpiderMan.{Requester, Response}
  alias SpiderMan.Pipeline.{SetCookie, DuplicateFilter}

  @impl true
  def settings do
    [
      downloader_options: [
        pipelines: [DuplicateFilter, SetCookie],
        requester:
          {Requester.Finch,
           [
             logging?: true,
             base_url: "https://www.example.com"
             # proxy: %{schema: :http, address: "127.0.0.1", port: 1087}
           ]}
      ],
      spider_options: [
        pipelines: [DuplicateFilter, SetCookie]
      ],
      item_processor_options: [
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
        items = Enum.map(1..10, &build_item(&1, &1))
        requests = Enum.map(1..10, &build_request("/#{&1}"))
        %{items: items, requests: requests}

      _ ->
        %{items: [], requests: []}
    end
  end
end
