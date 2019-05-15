#!/bin/bash
#
# Copyright (c) 2019 Mellanox Technologies. All rights reserved.
#
# This Software is licensed under one of the following licenses:
#
# 1) under the terms of the "Common Public License 1.0" a copy of which is
#    available from the Open Source Initiative, see
#    http://www.opensource.org/licenses/cpl.php.
#
# 2) under the terms of the "The BSD License" a copy of which is
#    available from the Open Source Initiative, see
#    http://www.opensource.org/licenses/bsd-license.php.
#
# 3) under the terms of the "GNU General Public License (GPL) Version 2" a
#    copy of which is available from the Open Source Initiative, see
#    http://www.opensource.org/licenses/gpl-license.php.
#
# Licensee has the right to choose one of the above licenses.
#
# Redistributions of source code must retain the above copyright
# notice and one of the license notices.
#
# Redistributions in binary form must reproduce both the above copyright
# notice, one of the license notices in the documentation
# and/or other materials provided with the distribution.
#
# Author: Alex Rosenbaum <alexr@mellanox.com>
#

if [[ debug == "$1" ]]; then
  INSTRUMENTING=yes  # any non-null will do
  shift
fi
echodbg () {
  [[ "$INSTRUMENTING" ]] && builtin echo $@
}

DEVS=$1
if [ -z "$DEVS" ] ; then
        DEVS=$(ls /sys/class/infiniband/)
fi

for dev in $DEVS ; do
        #echo -e "dev=$dev"
        for port in $(ls /sys/class/infiniband/$dev/ports/) ; do
                #echo -e "  port=$port"
                ll=$(cat /sys/class/infiniband/$dev/ports/$port/link_layer);
                #echo -e "    ll=$ll"
                if [ $ll = "Ethernet" ] ; then
                        ndev=$(cat /sys/class/infiniband/$dev/ports/$port/gid_attrs/ndevs/0)
                        ipaddr=$(ip -f inet addr show $ndev | grep -Po 'inet \K[\d.]+')
			if [ -z "$ipaddr" ] ; then
				ipaddr="[no ip addr]"
			fi
                        #echo -e "dev=$dev\tport=$port\tll=$ll\tndev=$ndev\tipaddr=$ipaddr"
                        mlx_pci_dev_path=$(readlink -f /sys/class/infiniband/$dev/device)
                        mlx_pci_dev=${mlx_pci_dev_path##*/}
                        mlx_pci_br_path=$(dirname $(dirname $mlx_pci_dev_path))
                        mlx_pci_br=${mlx_pci_br_path##*/}
                        echodbg -e "dev=$dev\tport=$port\tll=$ll\tndev=$ndev\tipaddr=$ipaddr\tpci_dev=${mlx_pci_dev_path##*/}\tpci_br=$mlx_pci_br"

                        for pci in $(ls /sys/class/pci_bus/) ; do
                                #echo -e "pci: $pci"
                                pci_dev_path=$(readlink -f /sys/class/pci_bus/$pci)
                                #echo -e "dev_path: $pci_dev_path"
                                pci_br_path=$(dirname $(dirname $(dirname $pci_dev_path)))
                                pci_br=${pci_br_path##*/}
                                #echo -e "br_path: $pci_br_path '$pci_br'"
                                if [ $mlx_pci_br = $pci_br ] ; then #same pci bridge
                                        if [ ${mlx_pci_dev:0:7} = $pci ] ; then
                                                #echo -e "ALEXR same pci dev ${mlx_pci_dev:0:7} $pci"
                                                continue
                                        fi
					pci_str=$(lspci -D -s $pci:00.0)
					if [ -z "$pci_str" ] ; then
						pci_str="$pci:00.0"
					fi
                                        echo -e "$ipaddr ($dev) is near $pci_str"
                                fi
                        done #pci
                fi
        done #port
done #dev

