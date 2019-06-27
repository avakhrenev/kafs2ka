{ pkgs ? import <nixpkgs> {} }: # ignore provided nixpkgs, fetch required version
let
  pkgsArguments = {
    allowUnfree = true;
    packageOverrides = ps: {
      jdk = ps.openjdk11;
      jre = ps.openjdk11;
      nodejs = ps.nodejs-10_x;
      # linuxPackages = ps.linuxPackages_4_18;
    };
  };
  unstable = (import ((builtins.fetchGit {
    url = https://github.com/NixOS/nixpkgs-channels.git;
    ref = "nixos-unstable";
    # to update, see the number of commits and the latest commit in
    # https://github.com/nixos/nixpkgs-channels/tree/nixos-unstable
    # 183,832 commits
    rev = "20b993ef2c9e818a636582ade9597f71a485209d";
  })) {
    config = pkgsArguments;
  });
in
with unstable.pkgs;
stdenv.mkDerivation {
  name = "kafs2ka-env";
  buildInputs = [
    jdk
    gitMinimal
    (sbt.overrideAttrs (oldAttrs: {
      patchPhase = ''
        ${oldAttrs.patchPhase}
        echo "# Enable grallvm
        -J-XX:+UnlockExperimentalVMOptions
        -J-XX:+EnableJVMCI
        -J-XX:+UseJVMCICompiler
        -J--illegal-access=deny
        -Djvmci.Compiler=graal" >> conf/sbtopts
      '';
    }))
    ncurses # sbt complains it can't find `tput` command
    which # for OS X
    python # for OS X
    bloop # to make use of sbt-bloop plugin
  ];
}
