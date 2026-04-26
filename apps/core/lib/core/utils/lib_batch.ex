defmodule Core.Utils.LibBatch do
  def get_batch(), do: :rocksdb.batch()

  def handle_batch_command({:delete, key}, batch, cf_handle), do: handle_delete(batch, key, cf_handle)
  def handle_batch_command({command, key, payload}, batch, cf_handle) do
    case command do
      :post ->
        handle_create(batch, key, cf_handle, payload)
      :update ->
        handle_update(batch, key, cf_handle, payload)
      :merge ->
        handle_merge(batch, key, cf_handle, payload)

      _ -> {:error, "Error: invalid command"}
    end
  end

  def make_batch(db_handle, cf_handles, batch_handle, count) do
    :ok = handle_merge(batch_handle, "count", cf_handles["meta"], {:int_add, count})
    :rocksdb.write_batch(db_handle, batch_handle, [])
  after
    :rocksdb.release_batch(batch_handle)
  end

  defp handle_create(batch, key, cf_handle, payload) do
    :rocksdb.batch_put(batch, cf_handle, key, payload)
  end

  defp handle_update(batch, key, cf_handle, payload) do
    :rocksdb.batch_put(batch, cf_handle, key, payload)
  end

  defp handle_delete(batch, key, cf_handle) do
    :rocksdb.batch_delete(batch, cf_handle, key)
  end

  defp handle_merge(batch, key, cf, {:int_add, count}) do
    :rocksdb.batch_merge(batch, cf, key, :erlang.term_to_binary({:int_add, count}))
  end

  defp handle_merge(batch, key, cf, {:list_append, terms}) do
    :rocksdb.batch_merge(batch, cf, key, :erlang.term_to_binary({:list_append, terms}))
  end

  defp handle_merge(batch, key, cf, {:list_subtract, terms}) do
    :rocksdb.batch_merge(batch, cf, key, :erlang.term_to_binary({:list_subtract, terms}))
  end

  defp handle_merge(batch, key, cf, {:list_set, {index, term}}) do
    :rocksdb.batch_merge(batch, cf, key, :erlang.term_to_binary({:list_set, index, term}))
  end

    defp handle_merge(batch, key, cf, {:list_delete, {start_index, end_index}}) do
    :rocksdb.batch_merge(batch, cf, key, :erlang.term_to_binary({:list_delete, start_index, end_index}))
  end

  defp handle_merge(batch, key, cf, {:list_delete, index}) do
    :rocksdb.batch_merge(batch, cf, key, :erlang.term_to_binary({:list_delete, index}))
  end

  defp handle_merge(batch, key, cf, {:list_insert, {place, terms}}) do
    :rocksdb.batch_merge(batch, cf, key, :erlang.term_to_binary({:list_insert, place, terms}))
  end

  defp handle_merge(batch, key, cf, {:binary_append, term}) do
    :rocksdb.batch_merge(batch, cf, key, :erlang.term_to_binary({:binary_append, term}))
  end

  defp handle_merge(batch, key, cf, {:binary_replace, {start_index, end_index, term}}) do
    :rocksdb.batch_merge(batch, cf, key, :erlang.term_to_binary({:binary_replace, start_index, end_index, term}))
  end

  defp handle_merge(batch, key, cf, {:binary_insert, {index, term}}) do
    :rocksdb.batch_merge(batch, cf, key, :erlang.term_to_binary({:binary_insert, index, term}))
  end

  defp handle_merge(batch, key, cf, {:binary_erase, {start_index, end_index}}) do
    :rocksdb.batch_merge(batch, cf, key, :erlang.term_to_binary({:binary_erase, start_index, end_index}))
  end

  defp handle_merge(_, _, _, {command, _}) do
    {:error, "Error: invalid payload for command batch#{inspect(command)}"}
  end
end
