defmodule LibIdTest do
  alias Core.StateMachine.RocksOwner
  alias Core.Utils.LibId

  use Core.Fixtures.RocksCase, async: true

  test "get_group_id returns correct group ID", %{db_handle: db_handle, cf_handles: cf_handles} do
    cf_handle = cf_handles.meta

    # Insert a test group into the database
    group_name = "test_group"
    group_id = Uniq.UUID.uuid7()
    :ok = :rocksdb.put(db_handle, cf_handle, group_name, group_id, [])

    # Test that get_group_id retrieves the correct group ID
    assert {:ok, ^group_id} = LibId.get_group_id(db_handle, cf_handle, group_name)
  end

  test "get_group_id returns error for non-existent group", %{db_handle: db_handle, cf_handles: cf_handles} do
    cf_handle = cf_handles.meta

    # Test that get_group_id returns an error for a non-existent group
    assert {:error, "not found"} = LibId.get_group_id(db_handle, cf_handle, "non_existent_group")
  end

  test "get_search_id (generated ids) returns correct search ID" do
    tenant_id = Uniq.UUID.uuid7()
    group_id = Uniq.UUID.uuid7()
    raw_tenant = tenant_id |> String.replace("-", "") |> Base.decode16!(case: :mixed)
    raw_group = group_id |> String.replace("-", "") |> Base.decode16!(case: :mixed)
    search_id = raw_tenant <> raw_group

    assert {:ok, ^search_id} = LibId.get_search_id(tenant_id, group_id)
  end

  test "get_search_id (normal way) returns correct search ID", %{db_handle: db_handle, cf_handles: cf_handles} do
    cf_handle = cf_handles.meta

    group_name = "test_group"
    group_id = Uniq.UUID.uuid7()
    :ok = :rocksdb.put(db_handle, cf_handle, group_name, group_id, [])

    tenant_id = Uniq.UUID.uuid7()
    raw_tenant = tenant_id |> String.replace("-", "") |> Base.decode16!(case: :mixed)

    {:ok, group_id} = LibId.get_group_id(db_handle, cf_handle, "test_group")
    raw_group = group_id |> String.replace("-", "") |> Base.decode16!(case: :mixed)
    search_id = raw_tenant <> raw_group
    assert {:ok, ^search_id} = LibId.get_search_id(tenant_id, group_id)
  end

  test "get_full_id returns correct full ID", %{db_handle: db_handle, cf_handles: cf_handles} do
    cf_handle = cf_handles.meta

    group_name = "test_group"
    group_id = Uniq.UUID.uuid7()
    :ok = :rocksdb.put(db_handle, cf_handle, group_name, group_id, [])

    tenant_id = Uniq.UUID.uuid7()
    job_id = Uniq.UUID.uuid7()
    raw_tenant = tenant_id |> String.replace("-", "") |> Base.decode16!(case: :mixed)
    raw_job = job_id |> String.replace("-", "") |> Base.decode16!(case: :mixed)

    {:ok, group_id} = LibId.get_group_id(db_handle, cf_handle, "test_group")
    raw_group = group_id |> String.replace("-", "") |> Base.decode16!(case: :mixed)
    full_id = raw_tenant <> raw_group <> raw_job

    assert {:ok, ^full_id} = LibId.get_full_id(tenant_id, group_id, job_id)
  end

  test "get_full_id returns error not decodable tenant ID" do
    group_id = Uniq.UUID.uuid7()
    job_id = Uniq.UUID.uuid7()
    assert {:error, "ID generation failed with errors: failed to decode ID: invalidtenantid"} =
             LibId.get_full_id("invalid-tenant-id", group_id, job_id)
  end

  test "get_full_id returns error not decodable group ID" do
    tenant_id = Uniq.UUID.uuid7()
    job_id = Uniq.UUID.uuid7()
    assert {:error, "ID generation failed with errors: failed to decode ID: invalidgroupid"} =
             LibId.get_full_id(tenant_id, "invalid-group-id", job_id)
  end

  test "get_full_id returns error not decodable job ID" do
    tenant_id = Uniq.UUID.uuid7()
    group_id = Uniq.UUID.uuid7()
    assert {:error, "ID generation failed with errors: failed to decode ID: invalidjobid"} =
             LibId.get_full_id(tenant_id, group_id, "invalid-job-id")
  end
end
