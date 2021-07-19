#!/bin/bash


if [ $(/usr/bin/id -u) -ne 0 ]; then 
    echo "Not running as root"     
    exit 
fi

NEW_ARGS=()
interf=$(route | grep '^default' | grep -o '[^ ]*$')
host=$(hostname --all-ip-addresses | awk '{ print $1 }')

filters=("frame.time_epoch" "ip.src" "ip.dst_host" "tcp.dstport")


for elem in "${filters[@]}"
do
NEW_ARGS+="-e ${elem} "
done

echo ${interf}
echo ${NEW_ARGS}

set -x
tshark -i ${interf} -lT ek -lT fields -E separator=, -E quote=d ${NEW_ARGS} -Y "ip.dst != 192.168.0.0/16 && ip.dst != 172.16.0.0/12 && ip.dst != 10.0.0.0/8 && ip.dst != 224.0.0.0/4 && tcp" > ./pcap/pcap.csv