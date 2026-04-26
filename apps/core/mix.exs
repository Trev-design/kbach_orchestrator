defmodule Core.MixProject do
  use Mix.Project

  def project do
    [
      app: :core,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Core.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ra, "~> 3.1"},
      {:rocksdb, "~> 2.5"},
      {:ex2ms, "~> 1.7"},
      {:gen_stage, "~> 1.3"},
      {:typed_struct, "~> 0.3.0"},
      {:uniq, "~> 0.6.2"}
    ]
  end
end
