{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  packages = [
    pkgs.poetry
    pkgs.python313
    pkgs.postgresql
    pkgs.openssl
  ];

  shellHook = ''
    # Create virtual environment if it doesn't exist
    if [ ! -d .venv ]; then
      echo "Creating new Poetry environment..."
      poetry config virtualenvs.in-project true
      poetry env use ${pkgs.lib.getExe pkgs.python313}
      poetry install --all-extras
    fi

    # Activate virtual environment
    source .venv/bin/activate
    echo "Python development environment ready!"
  '';
}
