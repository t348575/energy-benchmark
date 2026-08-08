#!/bin/zsh

if [ $# -ne 1 ]; then
    echo "Usage: $0 <read configs>"
    exit 1
fi

cargo b -p nvme-energy-bench --release

configs=()
while IFS= read -r line; do
    configs+=("$line")
done < $1

./precondition.sh /dev/nvme1n1

for f in ${configs[@]}; do
    cat /sys/block/nvme1n1/queue/max_hw_sectors_kb | sudo tee /sys/block/nvme1n1/queue/max_sectors_kb
    sudo -E RUST_LOG=debug numactl --cpunodebind=0 --membind=0 target/release/nvme-energy-bench -l common bench --skip-plot -c configs/$f.yaml
done