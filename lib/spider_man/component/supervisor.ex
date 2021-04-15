defmodule SpiderMan.Component.Supervisor do
  use Supervisor

  def process_name(spider), do: :"#{spider}.Component.Supervisor"

  def start_link(spider) do
    Supervisor.start_link(__MODULE__, [], name: process_name(spider))
  end

  @impl true
  def init(_arg) do
    Supervisor.init([], strategy: :one_for_one, max_restarts: 0)
  end
end
