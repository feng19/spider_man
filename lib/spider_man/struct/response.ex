defmodule SpiderMan.Response do
  @moduledoc false
  @enforce_keys [:key, :env]
  defstruct [:key, :env, :flag, options: [], retries: 0]

  @type t :: %__MODULE__{
          key: any,
          env: Tesla.Env.t(),
          options: keyword,
          retries: integer,
          flag: any
        }
end
