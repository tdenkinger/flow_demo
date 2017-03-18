defmodule FlowDemo do
  alias NimbleCSV.RFC4180, as: CSV

  def run(file) do
    sorted_lines =
    File.read!(file)
    |> CSV.parse_string
    |> Enum.reduce(%{download: [], stream: [], not_found: []}, fn(line, breakdown) ->
         case Enum.at(line, 2) do
           "Stream" ->
             Map.update!(breakdown, :stream, &([transform_line(line) | &1]))
           "Download" ->
             Map.update!(breakdown, :download, &([transform_line(line) | &1]))
           _ ->
             Map.update!(breakdown, :not_found, &([line | &1]))
         end
       end)

    File.write("downloads.txt", CSV.dump_to_iodata(sorted_lines[:download]))
    File.write("streams.txt",   CSV.dump_to_iodata(sorted_lines[:stream]))
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
