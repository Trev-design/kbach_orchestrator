defmodule Core.Utils.LibGet do
  alias Core.Utils.LibFormat

  def get_single_job(db_handle, cf_handle, id) do
    case :rocksdb.get(db_handle, cf_handle, id, single_get_options()) do
      {:ok, value} -> {:ok, value}
      {:error, reason} -> {:error, "Failed to get job for ID #{id}: #{reason}"}
    end
  end

  def get_multiple_jobs(db_handle, cf_handle, ids) do
    :rocksdb.multi_get(db_handle, cf_handle, ids, multi_get_options())
    |> Enum.map(fn {:ok, value}     -> {:ok, value}
                   :not_found       -> {:error, "Job not found"}
                   {:error, reason} -> {:error, "Failed to get, with error: #{reason}"}end)
    |> LibFormat.format_results()
  end

  def get_jobs_by_group(db_handle, cf_handle, group_id) do
    case :rocksdb.iterator(db_handle, cf_handle, iterator_options()) do
      {:ok, iterator} ->
        get_group_results([], iterator, group_id)

      {:error, reason} -> {:error, reason}
    end
  end

  defp get_group_results(list, iterator, id) do
    with {:ok, head}    <- start_iteration(list, iterator, id),
         {:ok, results} <- iterate(head, iterator)
    do
      {:ok, results}
    end
  end

  defp start_iteration(list, iterator, id) do
    case :rocksdb.iterator_move(iterator, {:seek, id}) do
      {:ok, key, value} -> {:ok, [{key, value} | list]}
      {:error, reason}  -> {:error, reason}
      _                 -> {:error, "something went wrong"}
    end
  end

  defp iterate(list, iterator) do
    case :rocksdb.iterator_move(iterator, :next) do
      {:ok, key, value} ->
        iterate([{key, value} | list], iterator)

      {:error, :invalid_iterator} ->
        get_iteration_result(iterator, {:ok, Enum.reverse(list)})

      {:error, reason} ->
        get_iteration_result(iterator, {:error, reason})

      _ -> get_iteration_result(iterator, {:error, "something went wrong"})
    end
  end

  defp get_iteration_result(iterator, result) do
    result
  after
    :rocksdb.iterator_close(iterator)
  end

  defp iterator_options() do
    [ prefix_same_as_start: true,
      fill_cache: false,
      total_order_seek: false,
      async_io: true ]
  end

  defp single_get_options() do
    [ fill_cache: false ]
  end

  defp multi_get_options() do
    [ {:async_io, true} | single_get_options() ]
  end
end
