defmodule SpiderMan.Pipeline.SaveToFile do
  require Logger
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(%{request: request, env: env} = data, dir) do
    case request.flag do
      flag when flag in [:save2file, :save2file_and_skip] ->
        key = request.key
        {path, result} = save2file(to_string(key), dir, env)

        Logger.info("SaveToFile key: #{key} file: #{path} result: #{result}")

        if flag == :save2file_and_skip do
          :skiped
        else
          %{data | env: path}
        end

      _ ->
        data
    end
  end

  @impl true
  def prepare_for_start(nil, options), do: prepare_for_start("data", options)
  def prepare_for_start(dir, options) when is_binary(dir), do: {dir, options}
  def prepare_for_start([dir: dir], options) when is_binary(dir), do: {dir, options}

  defp save2file(file_name, dir, env) do
    path = Path.join(dir, file_name)

    result =
      with :ok <- path |> Path.dirname() |> File.mkdir_p() do
        File.write(path, env.body)
      end

    {path, result}
  end
end
