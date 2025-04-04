import toml
import re

config=toml.load("build.config.toml")
workspace=toml.load("Cargo.toml")
bench_default=toml.load("benches/default-bench/Cargo.toml")
sensor_default=toml.load("sensors/default-sensor/Cargo.toml")

remove_entries = []
for member in workspace["workspace"]["members"]:
    if member != "benches/default-bench" and member.startswith("benches/"):
        remove_entries.append(member)
    elif member != "sensors/default-sensor" and member.startswith("sensors/"):
        remove_entries.append(member)

for item in remove_entries:
    workspace["workspace"]["members"].remove(item)

remove_entries = []
for dep in bench_default["dependencies"]:
    if "path" in bench_default["dependencies"][dep] and re.search("^[.]{2}/\\w", bench_default["dependencies"][dep]["path"]) is not None:
        remove_entries.append(dep)
for item in remove_entries:
    del bench_default["dependencies"][item]

remove_entries = []
for dep in sensor_default["dependencies"]:
    if "path" in sensor_default["dependencies"][dep] and re.search("^[.]{2}/\\w", sensor_default["dependencies"][dep]["path"]) is not None:
        remove_entries.append(dep)
for item in remove_entries:
    del sensor_default["dependencies"][item]

for bench in config["benches"]:
    workspace["workspace"]["members"].append("benches/" + bench)
    bench_default["dependencies"][bench] = {"path": "../" + bench}

for sensor in config["sensors"]:
    workspace["workspace"]["members"].append("sensors/" + sensor)
    sensor_default["dependencies"][sensor] = {"path": "../" + sensor}

f = open("Cargo.toml", "w")
toml.dump(workspace, f)

f = open("benches/default-bench/Cargo.toml", "w")
toml.dump(bench_default, f)

f = open("sensors/default-sensor/Cargo.toml", "w")
toml.dump(sensor_default, f)