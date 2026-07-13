{inputs, ...}: {
  perSystem = {pkgs, ...}: {
    checks.test = pkgs.buildZigPackage {
      src = inputs.inclusive.lib.inclusive ./.. [
        ../build.zig
        ../build.zig.zon
        ../src
      ];

      zigDepsHash = "sha256-T4fYRk5EjcbYUjxqYLiT67dBwolIv+6QJIkg2cAcDQg=";

      zigRelease = "ReleaseSafe";

      zigTarget = null;

      dontBuild = true;
      dontInstall = true;

      nativeCheckInputs = with pkgs; [sqlite];

      postCheck = ''
        touch $out
      '';
    };
  };
}
