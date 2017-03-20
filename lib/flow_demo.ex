defmodule FlowDemo do
  alias NimbleCSV.RFC4180, as: CSV

  def run(file) do
    files = open_files()

    File.stream!(file)
    |> CSV.parse_stream
    |> Stream.map(fn(line) -> {file_to_write_to(Enum.at(line, 2), files), prep_line(line)} end)
    |> Stream.each(fn({file, line}) -> IO.write(file, line) end)
    |> Stream.run
  end

  defp prep_line(line) do
    [transform_line(line)] |> CSV.dump_to_iodata
  end

  defp file_to_write_to("Stream", files),   do: files[:streams]
  defp file_to_write_to("Download", files), do: files[:downloads]
  defp file_to_write_to(_, files),          do: files[:notfound]

  defp open_files do
    %{streams:   File.open!("output/streams.txt",   [:write, :utf8]),
      downloads: File.open!("output/downloads.txt", [:write, :utf8]),
      notfound:  File.open!("output/notfound.txt",  [:write, :utf8]),
    }
  end

  defp transform_line(line) do
    [
      Enum.at(line, 0),
      Enum.at(line, 3),
      Enum.at(line, 4),
      calculate_total(Enum.at(line, 3), Enum.at(line, 4)),
      Enum.at(line, 1),
    ]
  end

  defp calculate_total(per, total_count) do
    String.to_float(per) * String.to_integer(total_count)
  end
end
