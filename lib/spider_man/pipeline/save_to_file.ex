defmodule SpiderMan.Pipeline.SaveToFile do
  @moduledoc """
  A post_pipeline what is use to download file directly for downloader component

  ## Usage
  ```elixir
  settings = [
    ...
    downloader_options: [
      post_pipeline: [#{inspect(__MODULE__)} | {#{inspect(__MODULE__)}, dir | [dir: dir]}]
    ]
  ]
  ```

  The file name is equal to request.key.
  If didn't set dir for this pipeline, the default is current dir.
  Set the flag for download a request:
  ```elixir
  build_request("https://www.example.com/download/file")
  |> set_key("file_name.txt")
  |> set_flag(:save2file)
  ```

  ### Supports Flag
  * `:save2file`: Save the request.body to file and continue go to next component.
  * `:save2file_and_skip`: Just save the request.body to file and break.
  """
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
