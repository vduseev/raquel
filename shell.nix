{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  packages = [
    pkgs.uv
    pkgs.python313
    pkgs.postgresql
    pkgs.openssl
  ];

  shellHook = ''
    # Create virtual environment if it doesn't exist
    if [ ! -d .venv ]; then
      echo "Creating new uv environment..."
      uv venv --python ${pkgs.lib.getExe pkgs.python313}
      uv sync
    fi

    # Activate virtual environment
    source .venv/bin/activate
    echo "Python development environment ready!"
  '';
}
