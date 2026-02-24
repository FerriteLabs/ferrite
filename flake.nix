{
  description = "Ferrite — High-performance tiered-storage key-value store";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        # Use the exact toolchain from rust-toolchain.toml
        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            rustToolchain
            pkgs.pkg-config
            pkgs.openssl
          ] ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
            pkgs.liburing
          ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.darwin.apple_sdk.frameworks.Security
            pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
          ];

          nativeBuildInputs = [
            pkgs.cargo-deny
            pkgs.cargo-audit
            pkgs.cargo-watch
            pkgs.cargo-release
            pkgs.cargo-edit
            pkgs.cargo-expand
          ];

          # Set library paths for linking
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
            pkgs.openssl
          ];

          shellHook = ''
            echo "🧲 Ferrite development environment"
            echo "   Rust:  $(rustc --version)"
            echo "   Cargo: $(cargo --version)"
            echo ""
            echo "   Run 'make help' for available commands"
          '';
        };
      }
    );
}
