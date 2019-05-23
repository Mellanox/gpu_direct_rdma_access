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
# Author: Michael Berezin <bichaelbe@mellanox.com>
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
                        link_st=$(ip -f inet link show $ndev | grep "state UP")
                        if [ -n "$link_st" ] ; then
                                echo "device $dev port $port ==> $ndev (Up) : ARP announce/ignore config"
				sysctl -w net.ipv4.conf.$ndev.arp_announce=1
				sysctl -w net.ipv4.conf.$ndev.arp_ignore=2
                        else
                                echo "device $dev port $port ==> $ndev (Down) : no config"
                        fi
                fi
        done #port
done #dev

