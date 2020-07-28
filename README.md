# GPU Direct RDMA Access example code
This package shows how to use the Mellanox DC QP to implement RDMA Read and Write operatinos directly to a remote GPU memory. It assumes the client appliation will run on a GPU enabled machine, like the NVIDIA DGX2. The server application, acting as a file storage simulation, will be running on few other Linux machines. All machine should have Mellanox ConnectX-5 NIC (or newer) in order for the DC QP to work properlly.

In the test codem the client application allocates memory on the defined GPU (flag '-u ) or on system RAM (default). Then sends a TCP request to the server application for a RDMA Write to the client's allocated buffer. Once the server application completes the RDMA Write operation it sends back a TCP 'done' message to the client. The client can loop for multiple such requests (flag '-n'). The RDMA message size can be configured (flag '-s' bytes)

For optimzed data transfer, the client requiers the GPU device selection based on PCI "B:D.F" format. It is recommened to chose a GPU which shares the same PCI bridge as the Mellanox ConectX NIC.

## Content:

gpu_direct_rdma_access.h, gpu_direct_rdma_access.c - Handles RDMA Read and Write ops from Server to GPU memory by request from the Client.
The API-s use DC type QPs connection for RDMA operations. The request to the server comes by socket.

gpu_mem_util.h, gpu_mem_util.c - GPU/CPU memory allocation

server.c, client.c - client and server main programs implementing GPU's Read/Write.

map_pci_nic_gpu.sh, arp_announce_conf.sh - help scripts

Makefile - makefile to build cliend and server execute files

## Installation Guide:

**1. MLNX_OFED**

Download MLNX_OFED-4.6-1.0.1.0 (or newer) from Mellanox web site: http://www.mellanox.com/page/products_dyn?product_family=26
Install with upstream libs
```sh
$ sudo ./mlnxofedinstall --force-fw-update --upstream-libs --dpdk
```
**2. CUDA libs**

Download CUDA Toolkit 10.1 (or newer) from Nvidia web site
```sh
$ wget https://developer.nvidia.com/compute/cuda/10.1/Prod/local_installers/cuda_10.1.105_418.39_linux.run
```
install on DGX server (GPU enabled servers)
```sh
$ sudo sh cuda_10.1.105_418.39_linux.run
```
**3. GPU Direct**

follow the download, build and inall guide on https://github.com/Mellanox/nv_peer_memory

**4. Multi-Homes network**

Configured system arp handling for multi-homed network with RoCE traffic (on DGX2 server)
```sh
$ git clone https://github.com/Mellanox/gpu_direct_rdma_access.git
$ ./write_to_gpu/arp_announce_conf.sh
```
**5. Check RDMA connectivity between all cluster nodes**

## Build Example Code:

```sh
$ git clone git@github.com:Mellanox/gpu_direct_rdma_access.git
$ cd gpu_direct_rdma_access
```
On the client machines
```sh
$ make USE_CUDA=1
```
On the server machines
```sh
$ make
```

## Run Server:
```sh
$ ./server -a 172.172.1.34 -n 10000 -D 1 -s 10000000 -p 18001 &
```

## Run Client:

We want to find the GPU's which share the same PCI bridge as the ConnectX Mellanox NIC
```sh
$ ./map_pci_nic_gpu.sh
172.172.1.112 (mlx5_12) is near 0000:b7:00.0 3D controller: NVIDIA Corporation Device 1db8 (rev a1)
172.172.1.112 (mlx5_12) is near 0000:b9:00.0 3D controller: NVIDIA Corporation Device 1db8 (rev a1)
172.172.1.113 (mlx5_14) is near 0000:bc:00.0 3D controller: NVIDIA Corporation Device 1db8 (rev a1)
172.172.1.113 (mlx5_14) is near 0000:be:00.0 3D controller: NVIDIA Corporation Device 1db8 (rev a1)
172.172.1.114 (mlx5_16) is near 0000:e0:00.0 3D controller: NVIDIA Corporation Device 1db8 (rev a1)
172.172.1.114 (mlx5_16) is near 0000:e2:00.0 3D controller: NVIDIA Corporation Device 1db8 (rev a1)
```

Run client application with matching IP address and BDF from the script output (-a and -u parameters)
```sh
$ ./client -t 0 -a 172.172.1.112 172.172.1.34 -u b7:00.0 -n 10000 -D 0 -s 10000000 -p 18001 &
<output>
```
