defmodule RocksOwnerTest do
  use Core.Fixtures.RocksCase, async: true

  alias Core.StateMachine.RocksOwner

  test "get_handles returns valid handles", %{db_handle: db_handle, cf_handles: cf_handles} do
     assert is_reference(db_handle)
     assert is_map(cf_handles)
     assert Map.has_key?(cf_handles, :default)
     assert Map.has_key?(cf_handles, :meta)
     assert Map.has_key?(cf_handles, :ephemeral)
     assert Map.has_key?(cf_handles, :transient)
     assert Map.has_key?(cf_handles, :permanent)
     assert Map.has_key?(cf_handles, :logs)
  end

  test "get_handles returns consistent handles across calls", %{rocks_owner: rocks_owner} do
    assert {:ok, handles1} = RocksOwner.get_handles(rocks_owner)
    assert {:ok, handles2} = RocksOwner.get_handles(rocks_owner)

    assert handles1.db_handle == handles2.db_handle
    assert Map.has_key?(handles1.cf_handles, :transient)
    assert Map.has_key?(handles1.cf_handles, :permanent)
    assert Map.has_key?(handles1.cf_handles, :logs)
  end
end
