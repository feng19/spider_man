defmodule SpiderMan.Pipeline.Standard do
  @moduledoc false
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(response, _state), do: response
  @impl true
  def prepare_for_start(arg, options), do: {arg, options}
  @impl true
  def prepare_for_stop(_arg), do: :ok
end

defmodule SpiderMan.Pipeline.Empty do
  @moduledoc false
  @behaviour SpiderMan.Pipeline
end

defmodule SpiderMan.Pipeline.OnlyCall do
  @moduledoc false
  @behaviour SpiderMan.Pipeline
  @impl true
  def call(response, _state), do: response
end

defmodule SpiderMan.Pipeline.NoCallFunction do
  @moduledoc false
  @behaviour SpiderMan.Pipeline

  def run(response, _state), do: response
end
