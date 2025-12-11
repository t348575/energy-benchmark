#!/bin/bash
for i in  /sys/class/*/*/device; do 
    pci=$(basename "$(readlink $i)")
    if [ -e $i/numa_node ]; then
        echo "NUMA Node: `cat $i/numa_node` ($i): `lspci -s $pci`" ;
    fi
done | sort
