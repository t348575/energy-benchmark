# IISWC 2026 NVMe Energy Bench Dataset

This dataset accompanies the IISWC 2026 paper *Characterizing Energy Efficiency Trade-offs in the Linux Storage Stack for Flash-based NVMe SSDs*. It contains the archived experiment configurations and measurement results used for the paper's data-replay workflow.

## Contents

The results tree contains one directory per experiment. Each experiment directory includes its `config.yaml`, `info.json`, and `data/` directory. The measurements cover workload, power, energy, throughput, IOPS, and latency results for the studied configurations.

This is a data archive, not a runnable benchmark installation. To regenerate the analysis and paper figures, use the source code artifact's replay workflow.

The experiment directories are suffixed by their run date. This can be used to identify which SSD the experiment corresponds to.
| Result directory timestamp range | SSD |
| --- | --- |
| Anything before 8th October 2025 | A |
| 25 November 2025 at 10:20:27 through 5 December 2025 at 11:44:53 | B |
| 8 October 2025 at 14:31:15 through 7 November 2025 at 11:51:15 | C |
| 7 November 2025 at 13:26:38 through 14 November 2025 at 10:37:01 | D |
| Anything after 5 December 2025 at 12:24:56 | D |
| 14 November 2025 at 11:44:36 through 25 November 2025 at 07:30:56 | E |

## Distribution and integrity

The Zenodo download is a multipart [BagIt](https://datatracker.ietf.org/doc/html/rfc8493)
archive. Its `data/packages/` directory contains Zstandard-compressed
`base_*.tar.zst` packages. The package membership and the top-level experiment
directories are recorded in `metadata/package_members.tsv` and
`metadata/base_directories.txt`.

## Software and replay instructions

The `nvme-energy-bench` source code and the full replay instructions are
available at:

<https://github.com/atlarge-research/nvme-energy-bench>

In that repository, see [artifact/README.md](https://github.com/atlarge-research/nvme-energy-bench/tree/main/artifact). For instructions for artifact download, extraction, replay workflow and optional remeasrement workflow.