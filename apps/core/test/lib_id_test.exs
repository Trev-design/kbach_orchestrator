defmodule LibIdTest do
  alias Core.StateMachine.RocksOwner
  alias Core.Utils.LibId

  use ExUnit.Case, async: true

  setup_all do
    args = [
      cache: cache,
      size: cache_size,
      id: "test",
      num_partitions: num_partitions,
      write_buffer_manager: wbm
    ]

    rocks_owner = start_supervised!({RocksOwner, args})

    on_exit(fn -> RocksOwner.terminate(rocks_owner) end)

    [rocks_owner: rocks_owner]
  end

  test "get_group_id returns correct group ID", %{rocks_owner: rocks_owner} do
    {:ok, handles} = RocksOwner.get_handles()
    db_handle = handles.db_handle
    cf_handle = handles.cf_handles["meta"]

    # Insert a test group into the database
    group_name = "test_group"
    group_id = Uniq.uuid7(:raw)
    :ok = :rocksdb.put(db_handle, cf_handle, group_name, group_id)

    # Test that get_group_id retrieves the correct group ID
    assert {:ok, ^group_id} = LibId.get_group_id(db_handle, cf_handle, group_name)
  end

  test "get_group_id returns error for non-existent group", %{rocks_owner: rocks_owner} do
    {:ok, handles} = RocksOwner.get_handles()
    db_handle = handles.db_handle
    cf_handle = handles.cf_handles["meta"]

    # Test that get_group_id returns an error for a non-existent group
    assert {:error, "not found"} = LibId.get_group_id(db_handle, cf_handle, "non_existent_group")
  end

  test "get_group_id returns error for corrupted data", %{rocks_owner: rocks_owner} do
    {:ok, handles} = RocksOwner.get_handles()
    db_handle = handles.db_handle
    cf_handle = handles.cf_handles["meta"]

    # Insert corrupted data into the database
    group_name = "corrupted_group"
    :ok = :rocksdb.put(db_handle, cf_handle, group_name, <<0, 1, 2>>)

    # Test that get_group_id returns an error for corrupted data
    assert {:error, _reason} = LibId.get_group_id(db_handle, cf_handle, group_name)
  end

  test "get_search_id (generated ids) returns correct search ID" do
    tenant_id = Uniq.uuid7()
    group_id = Uniq.uuid7()
    raw_tenant = tenant_id |> String.replace("-", "") |> Base.decode16!(case: :mixed)
    raw_group = group_id |> String.replace("-", "") |> Base.decode16!(case: :mixed)
    search_id = raw_tenant <> raw_group

    assert {:ok, ^search_id} = LibId.get_search_id(tenant_id, group_id)
  end

  test "get_search_id (normal way) returns correct search ID" do
    {:ok, handles} = RocksOwner.get_handles()
    db_handle = handles.db_handle
    cf_handle = handles.cf_handles["meta"]

    tenant_id = Uniq.uuid7()
    raw_tenant = tenant_id |> String.replace("-", "") |> Base.decode16!(case: :mixed)

    {:ok, group_id} = LibId.get_group_id(db_handle, cf_handle, "test_group")
    search_id = raw_tenant <> group_id
    assert {:ok, ^search_id} = LibId.get_search_id(tenant_id, group_id)
  end
end
