#! /bin/sh -eu

# make gcroot for each dependency of shell and run nix-shell
# source: https://github.com/NixOS/nix/issues/2208#issuecomment-412262911

mkdir -p .gcroots
nix-instantiate shell.nix --indirect --add-root $PWD/.gcroots/shell.drv
nix-store --indirect --add-root $PWD/.gcroots/shell.dep --realise $(nix-store --query --references $PWD/.gcroots/shell.drv)
exec nix-shell --pure --command "sbt; return" $(readlink $PWD/.gcroots/shell.drv)
