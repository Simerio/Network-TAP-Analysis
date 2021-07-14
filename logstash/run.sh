#!/bin/sh

set -x

if [[ $(/usr/bin/id -u) -ne 0 ]]; then 
    echo "Not running as root"     
    exit 
fi

#tshark -i wlo1 -lT ek -lT fields -E separator=, -E quote=d -e frame.time_epoch -e ip.src -e ip.dst_host -e tcp.dstport -e udp.dstport -Y "ip.dst != 192.168.1.3 && tcp " > ./pcap/pcap.json
tshark -i wlo1 -lT json -Y "ip.dst != 192.168.1.3" -N n > pcap.json