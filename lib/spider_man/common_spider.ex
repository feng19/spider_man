defmodule SpiderMan.CommonSpider do
  @moduledoc """
  A Common Spider what setting functions as callbacks instead of module defined

  ## Usage
      #{inspect(__MODULE__)}.start(:spider_name, handle_response: fn response, context -> %{} end)
  """
  use SpiderMan
  require Logger
  alias SpiderMan.{Engine, Request, Response, Item}

  @type init :: (Engine.state() -> Engine.state())
  @type handle_response ::
          (Response.t(), context :: map ->
             %{
               optional(:requests) => [Request.t()],
               optional(:items) => [Item.t()]
             })
  @type prepare_for_start ::
          (SpiderMan.prepare_for_start_stage(), Engine.state() -> Engine.state())
  @type prepare_for_stop :: (Engine.state() -> :ok)
  @type prepare_for_start_component ::
          (SpiderMan.component(), options :: keyword | false -> options :: keyword)
  @type prepare_for_stop_component :: (SpiderMan.component(), options :: keyword | false -> :ok)
  @type callback ::
          {:init, init}
          | {:handle_response, handle_response}
          | {:prepare_for_start, prepare_for_start}
          | {:prepare_for_stop, prepare_for_stop}
          | {:prepare_for_start_component, prepare_for_start_component}
          | {:prepare_for_start_component, prepare_for_start_component}
  @type callbacks :: [callback]

  @spec start(SpiderMan.spider(), callbacks, SpiderMan.settings()) :: Supervisor.on_start_child()
  def start(spider, callbacks, settings \\ []) do
    case check_callbacks_and_merge_settings(callbacks, settings) do
      {:ok, settings} -> SpiderMan.start(spider, settings)
      error -> error
    end
  end

  @spec check_callbacks_and_merge_settings(callbacks, SpiderMan.settings()) ::
          {:ok, SpiderMan.settings()} | {:error, String.t()}
  def check_callbacks_and_merge_settings(callbacks, settings \\ []) do
    with true <- Keyword.keyword?(callbacks),
         {:ok, callbacks} <- check_callbacks(callbacks) do
      settings = Keyword.merge(settings, spider_module: __MODULE__, callbacks: callbacks)
      {:ok, settings}
    else
      {nil, _} ->
        {:error, "Please defined :callbacks option when use #{inspect(__MODULE__)}."}

      false ->
        {:error, "Bad type of :callbacks option for #{inspect(__MODULE__)}, please use Keyword."}

      error ->
        error
    end
  end

  @impl true
  def handle_response(response, %{callback: callback} = context) do
    callback.(response, context)
  end

  @impl true
  def prepare_for_start(:pre, state) do
    state = setup_callbacks(state)

    if callback = Keyword.get(state.callbacks, :prepare_for_start) do
      callback.(:pre, state)
    else
      state
    end
  end

  def prepare_for_start(:post, state) do
    if callback = Keyword.get(state.callbacks, :prepare_for_start) do
      callback.(:post, state)
    else
      state
    end
  end

  @impl true
  def init(state) do
    if callback = Keyword.get(state.callbacks, :init) do
      callback.(state)
    else
      state
    end
  end

  @impl true
  def prepare_for_start_component(component, options) when is_list(options) do
    if callback = Keyword.get(options, :prepare_for_start) do
      callback.(component, options)
    else
      options
    end
  end

  def prepare_for_start_component(_component, options), do: options

  @impl true
  def prepare_for_stop_component(component, options) when is_list(options) do
    if callback = Keyword.get(options, :prepare_for_stop) do
      callback.(component, options)
    end
  end

  def prepare_for_stop_component(_component, options), do: options

  @impl true
  def prepare_for_stop(state) do
    if callback = Keyword.get(state.callbacks, :prepare_for_stop) do
      callback.(state)
    end
  end

  defmacrop check_callback(callbacks, key, arity) do
    quote do
      case Keyword.get(unquote(callbacks), unquote(key)) do
        callback when is_function(callback, unquote(arity)) ->
          {:ok, unquote(callbacks)}

        nil ->
          {:ok, unquote(callbacks)}

        other ->
          {:error,
           "Wrong type of #{unquote(key)}: #{inspect(other)} defined in :callbacks option when use #{inspect(__MODULE__)}, please use fun/#{unquote(arity)} for this option."}
      end
    end
  end

  defp check_callbacks(callbacks) do
    with callbacks <- Enum.reject(callbacks, &(elem(&1, 1) |> is_nil())),
         true <- Keyword.has_key?(callbacks, :handle_response),
         {:ok, callbacks} <- check_callback(callbacks, :handle_response, 2),
         {:ok, callbacks} <- check_callback(callbacks, :prepare_for_start, 2),
         {:ok, callbacks} <- check_callback(callbacks, :prepare_for_stop, 1),
         {:ok, callbacks} <- check_callback(callbacks, :init, 1),
         {:ok, callbacks} <- check_callback(callbacks, :prepare_for_start_component, 2),
         {:ok, callbacks} <- check_callback(callbacks, :prepare_for_stop_component, 2) do
      {:ok, callbacks}
    else
      false ->
        {:error,
         "Must defined :handle_response for :callbacks option when use #{inspect(__MODULE__)}."}

      error ->
        error
    end
  end

  defp setup_component_callback(state, key, callback) do
    %{
      state
      | downloader_options: Keyword.put_new(state.downloader_options, key, callback),
        spider_options: Keyword.put_new(state.spider_options, key, callback),
        item_processor_options: Keyword.put_new(state.item_processor_options, key, callback)
    }
  end

  defp setup_callbacks(state) do
    state =
      if callback = Keyword.get(state.callbacks, :prepare_for_start_component) do
        setup_component_callback(state, :prepare_for_start, callback)
      else
        state
      end

    state =
      if callback = Keyword.get(state.callbacks, :prepare_for_stop_component) do
        setup_component_callback(state, :prepare_for_stop, callback)
      else
        state
      end

    callback = Keyword.fetch!(state.callbacks, :handle_response)

    spider_options =
      Keyword.update(
        state.spider_options,
        :context,
        %{callback: callback},
        &Map.put_new(&1, :callback, callback)
      )

    %{state | spider_options: spider_options}
  end
end
