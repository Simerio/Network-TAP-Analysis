#!/bin/bash


if [ $(/usr/bin/id -u) -ne 0 ]; then 
    echo "Not running as root"     
    exit 
fi

NEW_ARGS=()

filters=("frame.time_epoch" "ip.src" "ip.dst_host" "tcp.dstport")


for elem in "${filters[@]}"
do
NEW_ARGS+="-e ${elem} "
done

echo ${NEW_ARGS}

set -x
tshark -i wlo1 -lT ek -lT fields -E separator=, -E quote=d ${NEW_ARGS} -Y "ip.dst != 192.168.1.3 && tcp " > ./pcap/pcap.csv
