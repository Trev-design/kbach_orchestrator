defmodule Core.CoreTypes.BatchType do
  @enforce_keys [:command, :key_data, :cf_name]
  defstruct [:command, :key_data, :value, :cf_name]
  @type t :: %__MODULE__{
          command: atom(),
          key_data: Core.CoreTypes.BatchKey.t(),
          value: any(),
          cf_name: String.t()
        }
end

defmodule Core.CoreTypes.BatchKey do
  @enforce_keys [:tenant_id, :group_id, :job_id]
  defstruct [:tenant_id, :group_id, :job_id]
  @type t :: %__MODULE__{
          tenant_id: String.t(),
          group_id: String.t(),
          job_id: String.t()
        }
end
