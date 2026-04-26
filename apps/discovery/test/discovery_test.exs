defmodule DiscoveryTest do
  use ExUnit.Case
  doctest Discovery

  test "greets the world" do
    assert Discovery.hello() == :world
  end
end
