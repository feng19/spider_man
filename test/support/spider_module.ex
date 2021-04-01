defmodule Spider1 do
  use SpiderMan

  defmodule Requester do
    @behaviour SpiderMan.Requester
    def request(url, _options) do
      {:ok, %Tesla.Env{url: url}}
    end
  end

  def test do
    SpiderMan.start(__MODULE__)
    Process.sleep(500)
    r = SpiderMan.Utils.build_request("/")
    SpiderMan.insert_requests(__MODULE__, [r])
  end

  @impl true
  def settings do
    [
      downloader_options: [
        finch_options: [
          base_url: "https://www.example.com",
          requester: Requester
        ]
      ]
      # item_pipeline_options: [batchers: []]
    ]
  end

  @impl true
  def handle_response(env, options) do
    if parent = Keyword.get(options, :parent) do
      send(parent, {:handle_response, env})
    end

    case env.url do
      "/" ->
        items = Enum.map(1..10, &build_item(&1, &1))
        requests = Enum.map(1..10, &build_request("/not_found/#{&1}"))
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

Spider1.test()
