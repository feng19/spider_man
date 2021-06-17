defmodule SpiderMan.Item do
  @moduledoc "Item Struct"
  @enforce_keys [:key, :value]
  defstruct [:key, :value, :flag, options: [], retries: 0]

  @type t :: %__MODULE__{
          key: any,
          value: any,
          options: keyword,
          retries: integer,
          flag: any
        }
end
