defmodule SpiderMan.Requester.DynamicFinch do
  @moduledoc false
  @behaviour SpiderMan.Requester

  def child_spec(init_arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [init_arg]},
      type: :supervisor
    }
  end

  def start_link(%{spider: spider, options: options, tid: tid}) do
    spec_options = options[:spec_options]
    adapter_options = options[:adapter_options]
    request_options = options[:request_options]

    children = [
      get_requester_spec(:A, spider, spec_options),
      get_requester_spec(:B, spider, spec_options)
    ]

    put_requester_settings(tid, spider, :A, adapter_options, request_options)
    opts = [strategy: :one_for_one, name: process_name(spider)]
    Supervisor.start_link(children, opts)
  end

  def process_name(spider), do: :"#{spider}.DynamicFinchSupervisor"
  def process_name(spider, id), do: :"#{spider}.DynamicFinch.#{id}"

  @impl true
  def request(url, options, %{spider: spider, tid: tid, middlewares: middlewares}) do
    setting = get_requester_settings(tid, spider)

    options = Keyword.merge(setting.request_options, options)

    middlewares
    |> Tesla.client({Tesla.Adapter.Finch, setting.adapter_options})
    |> Tesla.request([{:url, url} | options])
  end

  @impl true
  def prepare_for_start(_arg, downloader_options) do
    spider = Keyword.fetch!(downloader_options, :spider)
    finch_options = Keyword.get(downloader_options, :finch_options, [])

    finch_options =
      Keyword.merge(
        [
          spec_options: [pools: %{:default => [size: 32, count: 8]}],
          adapter_options: [pool_timeout: 5_000],
          request_options: [receive_timeout: 10_000],
          append_default_middlewares?: true,
          middlewares: [],
          logging?: false
        ],
        finch_options
      )

    middlewares =
      SpiderMan.Requester.Finch.append_default_middlewares(
        finch_options[:append_default_middlewares?],
        finch_options
      )

    tid = downloader_options[:common_pipeline_tid]
    context = %{requester: __MODULE__, middlewares: middlewares, tid: tid}
    finch_options = Keyword.put(finch_options, :middlewares, middlewares)
    sup_spec = {__MODULE__, %{spider: spider, options: finch_options, tid: tid}}

    downloader_options
    |> Keyword.update(:additional_specs, [sup_spec], &[sup_spec | &1])
    |> Keyword.update(:context, context, &Map.merge(&1, context))
  end

  defp get_requester_spec(id, spider, process_options) do
    finch_name = process_name(spider, id)
    spec = Finch.child_spec([{:name, finch_name} | process_options])
    %{spec | id: id}
  end

  defp get_requester_settings(tid, spider) do
    [{_, map}] = :ets.lookup(tid, {spider, __MODULE__})
    map
  end

  defp put_requester_settings(tid, spider, id, adapter_options, request_options) do
    name = process_name(spider, id)

    map = %{
      id: id,
      name: name,
      adapter_options: [{:name, name} | adapter_options],
      request_options: request_options
    }

    :ets.insert(tid, {{spider, __MODULE__}, map})
  end

  def switch_finch(spider, spec_options, adapter_options, request_options) do
    tid = :persistent_term.get({spider, :common_pipeline_tid})
    switch_finch(tid, spider, spec_options, adapter_options, request_options)
  end

  def switch_finch(tid, spider, spec_options, adapter_options, request_options) do
    sup = process_name(spider)
    %{id: now_id} = get_requester_settings(tid, spider)
    id = List.delete([:A, :B], now_id) |> hd()
    finch_spec = get_requester_spec(id, spider, spec_options)

    with :ok <- Supervisor.terminate_child(sup, id),
         :ok <- Supervisor.delete_child(sup, id),
         {:ok, _} = return <- Supervisor.start_child(sup, finch_spec) do
      put_requester_settings(tid, spider, id, adapter_options, request_options)
      return
    end
  end
end
