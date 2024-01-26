{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    systems.url = "github:nix-systems/default";
    devenv.url = "github:cachix/devenv/v0.6.3";

    gomod2nix = {
      url = "github:nix-community/gomod2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    devenv,
    systems,
    gomod2nix,
    ...
  } @ inputs: let
    forEachSystem = nixpkgs.lib.genAttrs (import systems);
  in {
    devShells = forEachSystem (system: let
      pkgs = nixpkgs.legacyPackages.${system};
      postgresPort = 5433;
      redisPort = 6380;
    in {
      default = devenv.lib.mkShell {
        inherit inputs pkgs;
        modules = [
          {
            packages = with pkgs; [
              automake
              go_1_21
              gomod2nix.legacyPackages.${system}.gomod2nix
              gotools
              golangci-lint
              go-tools
              gopls
              pre-commit
            ];

            enterShell = ''
              export TEST_DATABASE_URL="postgres://postgres:postgres@localhost:${toString postgresPort}/neoq?sslmode=disable&pool_max_conns=100"
              export TEST_REDIS_URL=localhost:${toString redisPort}
              export REDIS_PASSWORD=
            '';

            pre-commit.hooks.gomod2nix = {
              enable = true;
              always_run = true;
              name = "gomod2nix";
              description = "Run gomod2nix before commit";
              entry = "./bin/gomod2nix";
            };

            services = {
              postgres = {
                package = pkgs.postgresql;
                enable = true;
                listen_addresses = "127.0.0.1";
                port = postgresPort;
                initialScript = ''
                  CREATE USER postgres WITH PASSWORD 'postgres' SUPERUSER;
                  CREATE DATABASE neoq;
                '';
              };

              redis = {
                package = pkgs.redis;
                enable = true;
                port = redisPort;
              };
            };
          }
        ];
      };
    });
  };
}
