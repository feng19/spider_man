defmodule SpiderMan.Request do
  @moduledoc "Request Struct"
  @enforce_keys [:key, :url]
  defstruct [:key, :url, :flag, options: [], retries: 0]

  @type t :: %__MODULE__{
          key: any,
          url: binary,
          options: keyword,
          retries: integer,
          flag: any
        }
end
