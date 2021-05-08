defmodule SpiderMan.Item do
  @moduledoc false
  @enforce_keys [:key, :value]
  defstruct [:key, :value, :flag, options: [], retries: 0]

  @type t :: %__MODULE__{
          key: term,
          value: term,
          options: keyword,
          retries: integer,
          flag: any
        }
end
