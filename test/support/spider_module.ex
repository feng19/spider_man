defmodule Spider1 do
  use SpiderMan
  alias SpiderMan.{Response, Utils}
  alias SpiderMan.Pipeline.{SetCookie, DuplicateFilter}

  defmodule Requester do
    @behaviour SpiderMan.Requester
    @impl true
    def request(url, _options, _context) do
      {:ok, %Tesla.Env{url: url}}
    end
  end

  @impl true
  def settings do
    [
      # load_from_file: "./data/Spider1_1617359377",
      downloader_options: [
        # context: %{debug: true},
        pipelines: [DuplicateFilter, SetCookie],
        requester: Requester,
        finch_options: [
          logging?: true,
          base_url: "https://www.example.com"
        ]
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

  @impl true
  def prepare_for_start(state) do
    if parent = Map.get(state, :parent) do
      send(parent, :started)
    end

    r = Utils.build_request("/")
    SpiderMan.insert_requests(__MODULE__, [r])

    state
  end

  @impl true
  def prepare_for_start_component(_component, options) do
    options
  end

  @impl true
  def prepare_for_stop_component(_component, _options) do
    :ok
  end

  @impl true
  def prepare_for_stop(state) do
    if parent = Map.get(state, :parent) do
      send(parent, :stoped)
    end
  end
end
