# cmd
Execute an arbitrary command.

## Configuration
To use cmd, add `Cmd` as a bench, then specify the program and arguments:
```yaml
benches:
  - name: test
    repeat: 1
    bench:
      type: Cmd
      program: sleep
      args: ["5"]
```
