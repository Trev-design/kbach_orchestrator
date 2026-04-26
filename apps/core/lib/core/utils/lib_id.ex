defmodule Core.Utils.LibId do
  def get_group_id(db_handle, cf_handle, group_name) do
    case :rocksdb.get(db_handle, cf_handle, group_name) do
      {:ok, value}                    -> {:ok, value}
      :not_found                      -> {:error, "not found"}
      {:error, {:corruption, reason}} -> {:error, reason}
      {:error, reason}                -> {:error, reason}
    end
  end

  def get_split_id(tenant_id, group_id) do
    get_id([tenant_id, group_id])
  end

  def get_full_id(tenant_id, group_name, job_id) do
    get_id([tenant_id, group_name, job_id])
  end

  def get_search_id(tenant_id, group_id) do
    get_id([tenant_id, group_id])
  end

  defp get_id(ids) do
    ids
    |> Stream.map(fn id -> String.replace(id, "-", "") end)
    |> Stream.map(fn id ->
      case Base.decode16(id, case: :mixed) do
        {:ok, decoded} -> decoded
        :error         -> {:error, "failed to decode ID: #{id}"}
      end
    end)

    if Enum.any?(ids, fn id -> match?({:error, _}, id) end) do
      errors =
        ids
        |> Stream.filter(fn id -> match?({:error, _}, id) end)
        |> Enum.map(fn {:error, reason} -> reason end)

      {:error, "ID generation failed with errors: #{Enum.join(errors, ", ")}"}
    else
      {:ok, Enum.reduce(ids, <<>>, fn id, acc -> acc <> id end)}
    end
  end
end
