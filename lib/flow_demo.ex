defmodule FlowDemo do
  alias NimbleCSV.RFC4180, as: CSV

  def run(file) do
      parsed_data =
        File.read!(file)
        |> CSV.parse_string
        |> Enum.filter(fn(line) -> Enum.at(line, 2) == "Download" end)
        |> Enum.map(fn(line) -> transform_line(line) end)
        |> CSV.dump_to_iodata

      File.write!("test/data/downloads.csv", parsed_data)
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

  def calculate_total(per, total_count) do
    String.to_float(per) * String.to_integer(total_count)
  end
end
