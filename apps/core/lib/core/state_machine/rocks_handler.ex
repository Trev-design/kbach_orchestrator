defmodule Core.StateMachine.RocksHandler do
  @moduledoc """
  This module implements the :ra_machine behaviour,
  defining how to apply log entries to the Raft state machine using RocksDB operations.
  Each log entry is expected to be a tuple containing the operation type (e.g., :put, :delete, :get, :update)
  and the necessary parameters (e.g., key, value, column family name).
  The apply function handles each operation type by performing the corresponding RocksDB operation and returning the updated state
   and the result of the operation.
  The module also includes helper functions for handling batch operations,
  allowing for efficient application of multiple log entries in a single batch write to RocksDB.
  """
  @behaviour :ra_machine

  @enforce_keys [:db_handle, :cf_handles, :index]
  defstruct [:db_handle, :cf_handles, :index]
  @type t :: %__MODULE__{
          db_handle: reference(),
          cf_handles: map(),
          index: non_neg_integer()
        }

  alias Core.Utils.LibBatch
  alias Core.Utils.LibFormat
  alias Core.Utils.LibId
  alias Core.Utils.LibGet

  @impl true
  def init(conf), do: conf

  @impl true
  def apply(_meta, {:batch, batch_input}, %__MODULE__{} = state) do
    case LibFormat.format_batch_commands(batch_input, state) do
      {:ok, commands}         -> make_batch(commands, [], state)
      {:ok, commands, errors} -> make_batch(commands, errors, state)
    end
  end

  def apply(_meta, {:seek, tenant, group, cf_name}, state) do
    case LibFormat.format_key(group, state, fn group_id -> LibId.get_search_id(tenant, group_id) end) do
      {:ok, full_id} ->
        case LibGet.get_jobs_by_group(state.db_handle, state.cf_handles[cf_name], full_id) do
          {:ok, results} -> {state, {:ok, results}}
          {:error, reason} -> {state, {:error, reason}}
        end

      {:error, reason} -> {state, {:error, reason}}
    end
  end

   def apply(_meta, {:get_by_group, tenant, group, cf_name}, state) do
    case LibFormat.format_key(
      group,
      state,
      fn group_id -> LibId.get_search_id(tenant, group_id) end)
    do
      {:ok, full_id} ->
        case LibGet.get_jobs_by_group(state.db_handle, state.cf_handles[cf_name], full_id) do
          {:ok, results} -> {state, {:ok, results}}
          {:error, reason} -> {state, {:error, reason}}
        end

      {:error, reason} -> {state, {:error, reason}}
    end
  end

  def apply(_meta, {:get_single, tenant, group, job_id, cf_name}, state) do
    case LibFormat.format_key(
      group,
      state,
      fn group_id -> LibId.get_full_id(tenant, group_id, job_id) end)
    do
      {:ok, full_id} ->
        case LibGet.get_single_job(state.db_handle, state.cf_handles[cf_name], full_id) do
          {:ok, result} -> {state, {:ok, result}}
          {:error, reason} -> {state, {:error, reason}}
        end

      {:error, reason} -> {state, {:error, reason}}
    end
  end

   def apply(_meta, {:get_multi, tenant, group, job_ids, cf_name}, state) do
    case LibId.get_group_id(state.db_handle, state.cf_handles["meta"], group) do
      {:ok, group_id} ->
        case get_keys(tenant, group_id, job_ids) do
          {:ok, ids} ->
            make_multi_get(state, cf_name, ids, [])

          {:ok, ids, errors} ->
            make_multi_get(state, cf_name, ids, errors)
        end
    end
  end

  defp make_multi_get(state, cf_name, keys, errors) do
    case LibGet.get_multiple_jobs(state.db_handle, state.cf_handles[cf_name], keys) do
      {:ok, results} -> {state, {:ok, results, errors}}
      {:ok, results, get_errors} -> {state, {:ok, results, errors ++ get_errors}}
    end
  end

  defp make_batch(commands, errors, %__MODULE__{} = state) do
    with {:ok, batch_handle} <- :rocksdb.batch(),
          :ok <- make_batch_entries(commands, batch_handle),
          :ok <- LibBatch.make_batch(state.db_handle, state.cf_handles, batch_handle, length(commands))
    do
      {%__MODULE__{state | index: state.index + length(commands)}, :ok}
    else
      {:error, reason} -> {state, {:error, reason}}
    end
  end

  defp make_batch_entries(commands, batch) do
    commands
    |> Enum.each(fn {command, cf_name} ->
      LibBatch.handle_batch_command(command, batch, cf_name)
    end)
  end

  defp get_keys(tenant, group_id, job_ids) do
    job_ids
    |> Enum.map(fn job_id -> LibId.get_full_id(tenant, group_id, job_id) end)
    |> LibFormat.format_results()
  end
end
