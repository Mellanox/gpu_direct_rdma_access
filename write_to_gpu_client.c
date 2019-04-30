/*
 * Copyright (c) 2019 Mellanox Technologies, Inc.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <malloc.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <time.h>

#include "utils.h"
#include "gpu_mem_util.h"
#include "rdma_write_to_gpu.h"

static int debug = 1;
static int debug_fast_path = 1;
#define DEBUG_LOG if (debug) printf
#define DEBUG_LOG_FAST_PATH if (debug_fast_path) printf
#define FDEBUG_LOG if (debug) fprintf
#define FDEBUG_LOG_FAST_PATH if (debug_fast_path) fprintf

struct user_params {

    int                  port;
    int                  ib_port;
    unsigned long        size;
    char                *ib_devname;
    enum ibv_mtu         mtu;
    int                  iters;
    int                  gidx;
    int                  use_cuda;
    char                *servername;
};

/****************************************************************************************
 * Open socket connection on the client side, try to connect to the server by the given
 * IP address (servername). If success, return the connected socket file descriptor ID
 * Return value: socket fd - success, -1 - error
 ****************************************************************************************/
static int open_client_socket(const char *servername,
                              int         port)
{
    struct addrinfo *res,
                    *t;
    struct addrinfo hints = {
        .ai_family   = AF_UNSPEC,
        .ai_socktype = SOCK_STREAM
    };
    char   *service;
    int     ret_val;
    int     sockfd;

    if (asprintf(&service, "%d", port) < 0)
        return -1;

    ret_val = getaddrinfo(servername, service, &hints, &res);

    if (ret_val < 0) {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(ret_val), servername, port);
        free(service);
        return -1;
    }

    for (t = res; t; t = t->ai_next) {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0) {
            if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0) {
        fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        return -1;
    }

    return sockfd;
}

static void usage(const char *argv0)
{
    printf("Usage:\n");
    printf("  %s <host>     connect to server at <host>\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -p, --port=<port>         listen on/connect to port <port> (default 18515)\n");
    printf("  -d, --ib-dev=<dev>        use IB device <dev> (default first device found)\n");
    printf("  -i, --ib-port=<port>      use port <port> of IB device (default 1)\n");
    printf("  -s, --size=<size>         size of message to exchange (default 4096)\n");
    printf("  -m, --mtu=<size>          path MTU (default 1024)\n");
    printf("  -n, --iters=<iters>       number of exchanges (default 1000)\n");
    printf("  -g, --gid-idx=<gid index> local port gid index\n");
    printf("  -u, --use-cuda            use CUDA pacage (work with GPU memoty)\n");
}

static int parse_command_line(int argc, char *argv[], struct user_params *usr_par)
{
    memset(usr_par, 0, sizeof *usr_par);
    /*Set defaults*/
    usr_par->port       = 18515;
    usr_par->ib_port    = 1;
    usr_par->size       = 4096;
    usr_par->mtu        = IBV_MTU_1024;
    usr_par->iters      = 1000;
    usr_par->gidx       = -1;

    while (1) {
        int c;

        static struct option long_options[] = {
            { .name = "port",          .has_arg = 1, .val = 'p' },
            { .name = "ib-dev",        .has_arg = 1, .val = 'd' },
            { .name = "ib-port",       .has_arg = 1, .val = 'i' },
            { .name = "size",          .has_arg = 1, .val = 's' },
            { .name = "mtu",           .has_arg = 1, .val = 'm' },
            { .name = "iters",         .has_arg = 1, .val = 'n' },
            { .name = "gid-idx",       .has_arg = 1, .val = 'g' },
            { .name = "use-cuda",      .has_arg = 0, .val = 'u' },
            { 0 }
        };

        c = getopt_long(argc, argv, "p:d:i:s:m:n:g:u",
                        long_options, NULL);
        if (c == -1)
            break;

        switch (c) {
        
        case 'p':
            usr_par->port = strtol(optarg, NULL, 0);
            if (usr_par->port < 0 || usr_par->port > 65535) {
                usage(argv[0]);
                return 1;
            }
            break;

        case 'd':
            //usr_par->ib_devname = strdupa(optarg);
            usr_par->ib_devname = calloc(1, strlen(optarg)+1);
            if (!usr_par->ib_devname){
                perror("ib_devname mem alloc failure");
                return 1;
            }
            strcpy(usr_par->ib_devname, optarg);
            break;

        case 'i':
            usr_par->ib_port = strtol(optarg, NULL, 0);
            if (usr_par->ib_port < 0) {
                usage(argv[0]);
                return 1;
            }
            break;

        case 's':
            usr_par->size = strtol(optarg, NULL, 0);
            break;

        case 'm':
            usr_par->mtu = mtu_to_enum(strtol(optarg, NULL, 0));
            if (usr_par->mtu < 0) {
                usage(argv[0]);
                return 1;
            }
            break;

        case 'n':
            usr_par->iters = strtol(optarg, NULL, 0);
            break;

        case 'g':
            usr_par->gidx = strtol(optarg, NULL, 0);
            break;

        case 'u':
            usr_par->use_cuda = 1;
            break;
        
        default:
            usage(argv[0]);
            return 1;
        }
    }

    if (optind == argc) {
        fprintf(stderr, "Server name is missing in the commant line.\n");
        usage(argv[0]);
        return 1;
    } else if (optind == argc - 1) {
        //usr_par->servername = strdupa(argv[optind]);
        usr_par->servername = calloc(1, strlen(argv[optind])+1);
        if (!usr_par->servername){
            perror("servername mem alloc failure");
            return 1;
        }
        strcpy(usr_par->servername, argv[optind]);
    }
    else if (optind < argc) {
        usage(argv[0]);
        return 1;
    }

    if (!usr_par->ib_devname){
        fprintf(stderr, "IB device name is missing in the command line.");
        usage(argv[0]);
        return 1;
    }

    return 0;
}

int main(int argc, char *argv[])
{
    struct rdma_device     *rdma_dev;
    struct timeval          start;
    int                     cnt;
    struct user_params      usr_par;
    int                     ret_val = 0;
    int                     sockfd;

    srand48(getpid() * time(NULL));

    ret_val = parse_command_line(argc, argv, &usr_par);
    if (ret_val) {
        return ret_val;
    }
    
    DEBUG_LOG ("Connecting to remote server \"%s\"\n", usr_par.servername);
    sockfd = open_client_socket(usr_par.servername, usr_par.port);
    free(usr_par.servername);

    if (sockfd < 0) {
        if (usr_par.ib_devname) {
            free(usr_par.ib_devname);
        }
        return 1;
    }

    rdma_dev = rdma_open_device_target(usr_par.ib_devname, usr_par.ib_port); /* client */
    if (usr_par.ib_devname) {
        free(usr_par.ib_devname);
    }
    if (!rdma_dev) {
        ret_val = 1;
        goto clean_socket;
    }
    
    ret_val = rdma_set_lid_gid_from_port_info(rdma_dev, usr_par.gidx);
    if (ret_val) {
        goto clean_device;
    }

    ret_val = modify_target_qp_to_rtr(rdma_dev, usr_par.mtu);
    if (ret_val) {
        goto clean_device;
    }
    
    if (gettimeofday(&start, NULL)) {
        perror("gettimeofday");
        ret_val = 1;
        goto clean_device;
    }

    /* CPU or GPU memory buffer allocation */
    void    *buff;
#ifdef HAVE_CUDA
    buff = work_buffer_alloc(usr_par.size, usr_par.use_cuda);
#else
    buff = work_buffer_alloc(usr_par.size);
#endif //HAVE_CUDA
    if (!buff) {
        ret_val = 1;
        goto clean_device;
    }
    /* RDMA buffer registration */
    struct rdma_buffer *rdma_buff;

    rdma_buff = rdma_buffer_reg(rdma_dev, buff, usr_par.size);
    if (!rdma_buff) {
        ret_val = 1;
        goto clean_mem_buff;
    }

    /****************************************************************************************************
     * The main loop where client and server send and receive "iters" number of messages
     */
    for (cnt = 0; cnt < usr_par.iters; cnt++) {

        char desc_str[sizeof "0102030405060708:01020304:01020304:0102:010203:1:0102030405060708090a0b0c0d0e0f10"];
        char ackmsg[sizeof "rdma_write completed"];
        int  ret_size;
        
        // Sending RDMA data (address and rkey) by socket as a triger to start RDMA write operation
        ret_size = rdma_buffer_get_desc_str(rdma_buff, desc_str, sizeof desc_str);
        if (!ret_size) {
            ret_val = 1;
            goto clean_mem_buff;
        }
        
        DEBUG_LOG_FAST_PATH("Send message N %d: buffer desc \"%s\", device desc \"%s\"\n",
                            cnt, desc_str, desc_str + sizeof "0102030405060708:01020304:01020304");
        ret_size = write(sockfd, desc_str, sizeof desc_str);
        if (ret_size != sizeof desc_str) {
            perror("client write");
            fprintf(stderr, "Couldn't send RDMA data for iteration, write data size %d\n", ret_size);
            ret_val = 1;
            goto clean_mem_buff;
        }
        
        // Wating for confirmation message from the socket that rdma_write from the server has beed completed
        ret_size = recv(sockfd, ackmsg, sizeof ackmsg, MSG_WAITALL);
        if (ret_size != sizeof ackmsg) {
            perror("client read");
            fprintf(stderr, "Couldn't read \"rdma_write completed\" message, recv data size %d\n", ret_size);
            ret_val = 1;
            goto clean_mem_buff;
        }

        // Printing received data for debug purpose
        DEBUG_LOG_FAST_PATH("Received ack N %d: \"%s\"\n", cnt, ackmsg);
#ifdef HAVE_CUDA
        if (!usr_par.use_cuda) {
            DEBUG_LOG_FAST_PATH("Written data \"%s\"\n", (char*)buff);
        }
#else
        DEBUG_LOG_FAST_PATH("Written data \"%s\"\n", (char*)buff);
#endif //HAVE_CUDA
    }
    /****************************************************************************************************/

    ret_val = print_run_time(start, usr_par.size, usr_par.iters);
    if (ret_val) {
        goto clean_mem_buff;
    }

clean_mem_buff:
 #ifdef HAVE_CUDA
    work_buffer_free(buff, usr_par.use_cuda);
 #else
    work_buffer_free(buff);
 #endif //HAVE_CUDA

clean_device:
    rdma_close_device(rdma_dev);

clean_socket:
    close(sockfd);

    return ret_val;
}


