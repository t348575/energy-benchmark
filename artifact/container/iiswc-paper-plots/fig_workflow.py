import subprocess
import sys

python_runner = sys.executable

EXPERIMENTS = [
    "fig2a",
    "fig2b",
    "fig3and4",
    "fig5ab",
    "fig5cd",
    "fig6",
    "fig7and8",
    "fig9b",
    "fig10",
    "fig11",
]

# Run experiments
for experiment in EXPERIMENTS:
    print ("running experiment plot: ", experiment)
    proc = subprocess.Popen([python_runner, f"{experiment}.py"])
    proc.wait()
    print ("finished experiment plot: ", experiment)
