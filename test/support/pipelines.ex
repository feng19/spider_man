defmodule SpiderMan.Pipeline.Standard do
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(response, _state), do: response
  @impl true
  def prepare_for_start(arg, options), do: {arg, options}
  @impl true
  def prepare_for_stop(_arg), do: :ok
end

defmodule SpiderMan.Pipeline.Empty do
  @behaviour SpiderMan.Pipeline
end

defmodule SpiderMan.Pipeline.OnlyCall do
  @behaviour SpiderMan.Pipeline
  @impl true
  def call(response, _state), do: response
end

defmodule SpiderMan.Pipeline.NoCallFunction do
  @behaviour SpiderMan.Pipeline

  def run(response, _state), do: response
end
