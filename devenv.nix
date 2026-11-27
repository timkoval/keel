{
  pkgs,
  lib,
  ...
}:
{
  # https://devenv.sh/languages/
  languages.rust = {
    enable = true;
    # Channel and components live in rust-toolchain.toml. Do not also set
    # channel or components here; devenv treats that as a second toolchain.
    toolchainFile = ./rust-toolchain.toml;
  };

  # Tests and the merge path call git. SQLite is the bundled rusqlite feature,
  # so the shell does not need libsqlite or pkg-config.
  # rust-overlay's rustc links with the Cargo linker. An inherited host
  # clang has no crt objects (Scrt1.o, libgcc_s), so this shell pins Nix's gcc.
  packages = [
    pkgs.git
    pkgs.stdenv.cc
  ];

  enterShell = ''
    export CC=${lib.getExe' pkgs.stdenv.cc "cc"}
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="$CC"
  '';
}
