defmodule SpiderMan.CommonSpider do
  use SpiderMan
  require Logger

  @spec start(SpiderMan.spider(), callbacks :: [fun], SpiderMan.settings()) ::
          Supervisor.on_start_child()
  def start(spider, callbacks, settings \\ []) do
    with true <- Keyword.keyword?(callbacks),
         {:ok, callbacks} <- check_callbacks(callbacks) do
      settings = Keyword.merge(settings, spider_module: __MODULE__, callbacks: callbacks)
      SpiderMan.start(spider, settings)
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
      case Keyword.pop(unquote(callbacks), unquote(key)) do
        {callback, _} when is_function(callback, unquote(arity)) ->
          unquote(callbacks)

        {nil, _} ->
          unquote(callbacks)

        {other, callbacks} ->
          Logger.warn(
            "Wrong type of #{unquote(key)}: #{inspect(other)} defined in :callbacks option when use #{
              inspect(__MODULE__)
            }, please use fun/#{unquote(arity)} for this option."
          )

          callbacks
      end
    end
  end

  defp check_callbacks(callbacks) do
    case Keyword.get(callbacks, :handle_response) do
      callback when is_function(callback, 2) ->
        callbacks =
          callbacks
          |> check_callback(:prepare_for_start, 2)
          |> check_callback(:prepare_for_stop, 1)
          |> check_callback(:init, 1)
          |> check_callback(:prepare_for_start_component, 2)
          |> check_callback(:prepare_for_stop_component, 2)

        {:ok, callbacks}

      nil ->
        {:error,
         "Must defined :handle_response for :callbacks option when use #{inspect(__MODULE__)}."}

      other ->
        {:error,
         "Wrong type of handle_response: #{inspect(other)} defined in :callbacks option when use #{
           inspect(__MODULE__)
         }, please use fun/2 for this option."}
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
