defmodule Core.Fixtures.RocksCase do
  use ExUnit.CaseTemplate

  setup_all do
    args = Core.Fixtures.RocksPayload.get_payload()
    pid = start_supervised!({Core.StateMachine.RocksOwner, args})
    {:ok, handles} = Core.StateMachine.RocksOwner.get_handles(pid)
    {prefix, keys} = standard_get_input(handles.db_handle, handles.cf_handles.meta)
    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)
    [rocks_owner: pid,keys: {prefix, keys}, db_handle: handles.db_handle, cf_handles: handles.cf_handles]
  end

  defp standard_get_input(db_handle, cf_handle) do
    {prefix, payloads} =
      {Uniq.UUID.uuid7(:raw), Uniq.UUID.uuid7(:raw)}
      |> get_first_group_payload()

    ids =
      payloads
      |> Enum.map(fn {id, payload} ->
        :ok = :rocksdb.put(db_handle, cf_handle, id, payload, [])
        id
      end)

    {prefix, ids}
  end

  defp get_first_group_payload({tenant_id, group_id}) do
    {tenant_id <> group_id, for _i <- 1..20 do
      job = Uniq.UUID.uuid7(:raw)
      payload = :crypto.strong_rand_bytes(64)

      {tenant_id <> group_id <> job, payload}
    end}
  end
end
