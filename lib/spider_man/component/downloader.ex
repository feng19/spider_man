defmodule SpiderMan.Component.Downloader do
  @moduledoc false
  use SpiderMan.Component.Builder
  require Logger
  alias Broadway.Message
  alias SpiderMan.{Response, Pipeline, Utils}

  @impl true
  def handle_message(_processor, message, %{spider: spider} = context) do
    data = message.data

    if context[:debug] do
      Logger.debug("Downloader get message: #{inspect(data)}", spider: spider)
    end

    with %{url: url, options: options} = request <-
           Pipeline.call(context.pipelines, data, spider),
         {:ok, env} <- context.requester.request(url, options, context),
         :ok <- handle_response(env, request, context) do
      %{message | data: :ok}
    else
      :skiped ->
        Message.failed(message, :skiped)

      {:error, reason} ->
        Message.failed(message, reason)
    end
  end

  defp handle_response(
         env,
         %{key: key, flag: flag},
         %{next_tid: next_tid, tid: tid} = context
       ) do
    if flag in Map.get(context, :save2file_flags, []) do
      # save2file
      path =
        Map.get(context, :save2dir, "data")
        |> Path.join(to_string(key))

      result =
        with :ok <- path |> Path.dirname() |> File.mkdir_p() do
          File.write(path, env.body)
        end

      Logger.info("Downloader key: #{key} save to file: #{path} result: #{result}",
        spider: context.spider
      )

      result
    else
      # push successful events to next_tid
      Utils.push_events_to_next_producer_ets(next_tid, tid, [
        %Response{key: key, env: env, flag: flag}
      ])

      :ok
    end
  end
end
