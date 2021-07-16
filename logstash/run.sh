#!/bin/bash


if [ $(/usr/bin/id -u) -ne 0 ]; then 
    echo "Not running as root"     
    exit 
fi

NEW_ARGS=()
interf=$(route | grep '^default' | grep -o '[^ ]*$')
host=$(hostname --all-ip-addresses)

filters=("frame.time_epoch" "ip.src" "ip.dst_host" "tcp.dstport" "tcp.stream")


for elem in "${filters[@]}"
do
NEW_ARGS+="-e ${elem} "
done

echo ${interf}
echo ${NEW_ARGS}

set -x
tshark -i ${interf} -lT ek -lT fields -E separator=, -E quote=d ${NEW_ARGS} -Y "ip.dst != ${host} && tcp " > ./pcap/pcap.csv
