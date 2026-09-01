# Install

uv:

```bash
uv sync
```

Fallback with pip:

```bash
# Important make sure to pin to Python 3.9 first 
pip3 intall -r requirements.txt
```

```bash
mkdir -p plots
```

# Why these are separate plots?

All of the plots in the paper can be generated using nvme-energy-bench. However, in the paper we want to highlight certain relations in more detail and make the plots more appealing. Furthermore, we want to reduce the datasize significantly. As a result, the paper includes slightly different plots.

While we recommend using e-bench's scripts, we include the analysis and plotting scripts in this directory for reproducibility.  

# nvme-energy-bench-paper-plots

## Retrieve datasets

Recover preprocessed data:

```bash
pushd iiswcdata
for zip in $(ls preprocessed*.zip); do 
    unzip $zip; 
done
popd
```

What happened in preprocessing? (in case data is present):

```bash
# The following lines can not be run. The preprocess was used to generate the dataset from the dataruns (dataruns not included in repository, so it will fail) 
uv run preprocess-data.py
stat iiswcdata/preprocessed-ebench-data.json
zip -r iiswcdata/preprocessed-ebench-data.json.zip iiswcdata/preprocessed-ebench-data.json

uv run fig5cd_preprocess.py
stat iiswcdata/preprocessed-fig5cd*
for json in $(ls iiswcdata/*fig5cd*.json); do zip -r $json.zip $json; done
```

## Fig worklow

The following will run all experiments in sequence. For individual experiments look at the next section.

```bash
uv run fig_workflow.py
```

## Run figs separately

Fig 2 power range plots (hardcoded numbers):

```bash
uv run fig2a.py
uv run fig2b.py
```

Fig 3 and Fig 4 random read scaling:

```bash
uv run fig3and4.py
```

Fig 5 access patterns (PARTIALLY hardcoded numbers):

```bash
uv run fig5ab.py
uv run fig5cd.py
```

Fig 6 I/O interface:

```bash
uv run fig6.py | grep -E 'fig3-qd-*-bw-SCALING-cpu-*-e'
```

Fig 7 and Fig 8 DVFS:

```bash
uv run fig7and8.py
```

Fig 9 PS:

```bash
# Fig9a is generated with fig3
uv run fig3and4.py | grep -E '.*bw/ssd/.*PS-qd.*'
uv run fig9b.py
```

Fig 10 XFS (hardcoded numbers):

```bash
uv run fig10.py
```

Fig 11 Filebench:

```bash
uv run fig11.py 
```
