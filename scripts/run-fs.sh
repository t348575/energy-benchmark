#!/bin/bash

source verify_device.sh

configs=()
while IFS= read -r line; do
    configs+=("$line")
done < configs/experiments.txt

fs_list=(Ext4 Xfs F2fs)
dist_list=(full 16k 1g 1m)
SKIP_PREPARE=false

usage() {
  cat <<EOF
Usage: $0 [--fs "Ext4 Xfs F2fs"] [--dist "full 16k 1g 1m"] [--skip-prepare]

  --fs "LIST"          Space-separated list of filesystems (default: "Ext4 Xfs F2fs")
  --dist "LIST"        Space-separated list of distributions (default: "full 16k 1g 1m")
  --skip-prepare       Skip BOTH cleanup() and all preconditioning steps
  -h, --help           Show this help and exit
EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --fs)
      if [[ $# -lt 2 ]]; then
        echo "Error: --fs requires an argument" >&2
        exit 1
      fi
      IFS=' ' read -r -a fs_list <<< "$2"
      shift 2
      ;;
    --dist|--dists|--distributions)
      if [[ $# -lt 2 ]]; then
        echo "Error: --dist requires an argument" >&2
        exit 1
      fi
      IFS=' ' read -r -a dist_list <<< "$2"
      shift 2
      ;;
    --skip-prepare)
      SKIP_PREPARE=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

for f in "${fs_list[@]}"; do
  for d in "${dist_list[@]}"; do
    precond_ran=false

    if [[ "$SKIP_PREPARE" == false ]]; then
      echo "Cleaning up!"
      cleanup

      rm -f configs/precond.yaml

      # 1m and 1g with stage1 + stage2
      if [[ "$d" == "1m" || "$d" == "1g" ]]; then
        echo "Multistage precond starting!"
        for stage in stage1 stage2; do
          precond_src="configs/precond-create-fs-${d}-${stage}.yaml"
          if [[ ! -f "$precond_src" ]]; then
            echo "Error: Precond file '$precond_src' not found. Aborting." >&2
            exit 1
          fi

          sed "s/fs: Ext4/fs: ${f}/g" "$precond_src" > configs/precond.yaml
          precond_ran=true
          run_bench configs/precond.yaml
        done
      else
        echo "Single stage precond starting!"
        precond_src="configs/precond-create-fs-${d}.yaml"
        if [[ ! -f "$precond_src" ]]; then
          echo "Error: Precond file '$precond_src' not found. Aborting." >&2
          exit 1
        fi

        sed "s/fs: Ext4/fs: ${f}/g" "$precond_src" > configs/precond.yaml
        precond_ran=true
        run_bench configs/precond.yaml
      fi

      if [[ "$precond_ran" == true ]]; then
        sleep 60
      fi
    fi

    if [[ "$d" == "16k" || "$d" == "1m" ]]; then
      echo "Moving files!"
      sudo mount /dev/nvme1n1 mountpoint
      src=mountpoint/fileset
      dst=mountpoint/otherfiles

      mkdir -p $dst

      total=$(find $src -maxdepth 1 -type f | wc -l)
      leave=10000
      move=$((total - leave))
      if ((move > 0)); then
        find $src -maxdepth 1 -type f | head -n $move | while read f; do
          mv "$f" $dst
        done
      else
        echo "No files to move!"
      fi
      sudo umount /dev/nvme1n1 || true
    fi

    echo "Running configs!"
    for conf in "${configs[@]}"; do
      config_path="configs/$conf.yaml"
      if [[ ! -f "$config_path" ]]; then
        echo "Error: Config file '$config_path' not found. Skipping..."
        continue
      fi
      echo "Starting $config_path!"
      run_bench "$config_path"
    done
  done
done
