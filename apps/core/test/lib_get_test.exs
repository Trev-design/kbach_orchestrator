defmodule LibGetTest do
  alias Core.Utils.LibGet
  use Core.Fixtures.RocksCase, async: true


  test "get returns correct jobs", %{keys: {_prefix, ids}, db_handle: db_handle, cf_handles: cf_handles} do
    refute false = ids
      |> Enum.map(fn id -> LibGet.get_single_job(db_handle, cf_handles.meta, id) end)
      |> Enum.any?(fn result -> match?({:error, _}, result) end)
  end

  test "get returns error for non-existent job", %{db_handle: db_handle, cf_handles: cf_handles} do
    assert {:error, _reason} = LibGet.get_single_job(db_handle, cf_handles.meta, "non_existent_id")
  end

  test "multiple gets return correct jobs", %{keys: {_prefix, ids}, db_handle: db_handle, cf_handles: cf_handles} do
    assert {:ok, _results} = LibGet.get_multiple_jobs(db_handle, cf_handles.meta, ids)
  end

  test "get prefixed jobs", %{keys: {prefix, _keys}, db_handle: db_handle, cf_handles: cf_handles} do
    assert {:ok, _results} = LibGet.get_jobs_by_group(db_handle, cf_handles.meta, prefix)
  end
end
