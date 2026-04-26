defmodule Core.StateMachine.LogPipe do
  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(init_arg) do

  end
end
