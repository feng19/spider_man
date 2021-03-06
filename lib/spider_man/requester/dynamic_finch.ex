defmodule SpiderMan.Requester.DynamicFinch do
  @moduledoc false
  alias SpiderMan.Requester
  alias SpiderMan.Requester.Finch, as: RequesterFinch
  @behaviour Requester

  @default_options RequesterFinch.default_options()

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
  def prepare_for_start(finch_options, downloader_options) do
    finch_options = finch_options || []
    spider = Keyword.fetch!(downloader_options, :spider)

    finch_options =
      RequesterFinch.default_options()
      |> Keyword.merge(finch_options)
      |> RequesterFinch.handle_proxy_option()

    middlewares =
      Requester.append_default_middlewares(
        finch_options[:append_default_middlewares?],
        finch_options
      )

    tid = downloader_options[:common_pipeline_tid]
    context = %{requester: __MODULE__, middlewares: middlewares, tid: tid}
    finch_options = Keyword.put(finch_options, :middlewares, middlewares)
    sup_spec = {__MODULE__, %{spider: spider, options: finch_options, tid: tid}}

    producer =
      case Keyword.fetch!(downloader_options, :producer) do
        {producer, producer_options} ->
          producer_options =
            Keyword.update(producer_options, :additional_specs, [sup_spec], &[sup_spec | &1])

          {producer, producer_options}

        producer ->
          {producer, [additional_specs: [sup_spec]]}
      end

    downloader_options
    |> Keyword.put(:producer, producer)
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

  def switch_finch(spider, conn_opts: conn_opts) do
    switch_finch(spider, pool_opts: [conn_opts: conn_opts])
  end

  def switch_finch(spider, pool_opts: opts) do
    spec_options =
      Keyword.get(@default_options, :spec_options)
      |> Keyword.update!(
        :pools,
        &Map.update!(
          &1,
          :default,
          fn default_opts -> Keyword.merge(default_opts, opts) end
        )
      )

    switch_finch(spider, spec_options)
  end

  def switch_finch(spider, spec_options) do
    adapter_options = Keyword.get(@default_options, :adapter_options)
    request_options = Keyword.get(@default_options, :request_options)
    switch_finch(spider, spec_options, adapter_options, request_options)
  end

  def switch_finch(spider, spec_options, adapter_options, request_options) do
    :persistent_term.get(spider)
    |> Map.get(:common_pipeline_tid)
    |> switch_finch(spider, spec_options, adapter_options, request_options)
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
