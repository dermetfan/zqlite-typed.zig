{inputs, ...}: {
  imports = with inputs; [
    make-shell.flakeModules.default
  ];

  perSystem.make-shells.default = {pkgs, ...}: {
    imports = [inputs.utils.shellModules.zig];

    packages = with pkgs; [sqlite.dev];
  };
}
