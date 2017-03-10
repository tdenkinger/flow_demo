defmodule FlowDemo do
  alias NimbleCSV.RFC4180, as: CSV

  def run(file) do
    File.stream!(file)
    |> CSV.parse_stream
    |> Flow.from_enumerable
    |> Flow.filter(fn(line) -> Enum.at(line, 2) == "Download" end)
    |> Flow.map(fn(line) -> transform_line(line) end)
    |> CSV.dump_to_stream
    |> Stream.into(File.stream!("test/data/downloads.csv"))
    |> Stream.run
  end

  defp transform_line(line) do
    total = calculate_total(Enum.at(line, 3), Enum.at(line, 4))

    [
      Enum.at(line, 0),
      Enum.at(line, 3),
      Enum.at(line, 4),
      total,
      Enum.at(line, 1),
    ]
  end

  defp calculate_total(per, total_count) do
    String.to_float(per) * String.to_integer(total_count)
  end
end
