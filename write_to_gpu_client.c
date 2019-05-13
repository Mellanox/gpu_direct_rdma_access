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

extern int debug;
extern int debug_fast_path;

#define DEBUG_LOG if (debug) printf
#define DEBUG_LOG_FAST_PATH if (debug_fast_path) printf
#define FDEBUG_LOG if (debug) fprintf
#define FDEBUG_LOG_FAST_PATH if (debug_fast_path) fprintf

struct user_params {

    int                 port;
    unsigned long       size;
    int                 iters;
    int                 use_cuda;
    char               *bdf;
    char               *servername;
    struct sockaddr     hostaddr;
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
    printf("  -a, --addr=<ipaddr>       ip address of the local host net device <ipaddr v4> (mandatory)\n");
    printf("  -p, --port=<port>         listen on/connect to port <port> (default 18515)\n");
    printf("  -s, --size=<size>         size of message to exchange (default 4096)\n");
    printf("  -n, --iters=<iters>       number of exchanges (default 1000)\n");
    printf("  -u, --use-cuda=<BDF>      use CUDA pacage (work with GPU memoty),\n"
           "                            BDF corresponding to CUDA device, for example, \"3e:02.0\"\n");
    printf("  -D, --debug-mask=<mask>   debug bitmask: bit 0 - debug print enable,\n"
           "                                           bit 1 - fast path debug print enable\n");
}

static int parse_command_line(int argc, char *argv[], struct user_params *usr_par)
{
    memset(usr_par, 0, sizeof *usr_par);
    /*Set defaults*/
    usr_par->port       = 18515;
    usr_par->size       = 4096;
    usr_par->iters      = 1000;

    while (1) {
        int c;

        static struct option long_options[] = {
            { .name = "addr",          .has_arg = 1, .val = 'a' },
            { .name = "port",          .has_arg = 1, .val = 'p' },
            { .name = "size",          .has_arg = 1, .val = 's' },
            { .name = "iters",         .has_arg = 1, .val = 'n' },
            { .name = "use-cuda",      .has_arg = 1, .val = 'u' },
            { .name = "debug-mask",    .has_arg = 1, .val = 'D' },
            { 0 }
        };

        c = getopt_long(argc, argv, "a:p:s:n:u:D:",
                        long_options, NULL);
        if (c == -1)
            break;

        switch (c) {
        
        case 'a':
            get_addr(optarg, (struct sockaddr *) &usr_par->hostaddr);
            break;

        case 'p':
            usr_par->port = strtol(optarg, NULL, 0);
            if (usr_par->port < 0 || usr_par->port > 65535) {
                usage(argv[0]);
                return 1;
            }
            break;

        case 's':
            usr_par->size = strtol(optarg, NULL, 0);
            break;

        case 'n':
            usr_par->iters = strtol(optarg, NULL, 0);
            break;

        case 'u':
            usr_par->use_cuda = 1;
            usr_par->bdf = calloc(1, strlen(optarg)+1);
            if (!usr_par->bdf){
                perror("BDF mem alloc failure");
                return 1;
            }
            strcpy(usr_par->bdf, optarg);
            break;
        
        case 'D':
            debug           = (strtol(optarg, NULL, 0) >> 0) & 1; /*bit 0*/
            debug_fast_path = (strtol(optarg, NULL, 0) >> 1) & 1; /*bit 1*/
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
        ret_val = 1;
        /* We don't exit here, because when parse_command_line failed, probably
           some of memory allocations were completed, so we need to free them */
        goto clean_usr_par;
    }
    
    if (!usr_par.hostaddr.sa_family) {
        fprintf(stderr, "host ip address is missing in the command line.");
        usage(argv[0]);
        ret_val = 1;
        goto clean_usr_par;
    }

    printf("Connecting to remote server \"%s:%d\"\n", usr_par.servername, usr_par.port);
    sockfd = open_client_socket(usr_par.servername, usr_par.port);
    free(usr_par.servername);

    if (sockfd < 0) {
        ret_val = 1;
        goto clean_usr_par;
    }

    printf("Opening rdma device\n");
    rdma_dev = rdma_open_device_target(&usr_par.hostaddr); /* client */
    if (!rdma_dev) {
        ret_val = 1;
        goto clean_socket;
    }
    
    /* CPU or GPU memory buffer allocation */
    void    *buff;
    buff = work_buffer_alloc(usr_par.size, usr_par.use_cuda, usr_par.bdf);
    if (!buff) {
        ret_val = 1;
        goto clean_device;
    }
    
    /* We don't need bdf any more, sio we can free this. */
    if (usr_par.bdf) {
        free(usr_par.bdf);
        usr_par.bdf = NULL;
    }
    
    /* RDMA buffer registration */
    struct rdma_buffer *rdma_buff;

    rdma_buff = rdma_buffer_reg(rdma_dev, buff, usr_par.size);
    if (!rdma_buff) {
        ret_val = 1;
        goto clean_mem_buff;
    }

    char desc_str[256];
    int  ret_desc_str_size = rdma_buffer_get_desc_str(rdma_buff, desc_str, sizeof desc_str);
    if (!ret_desc_str_size) {
        ret_val = 1;
        goto clean_rdma_buff;
    }
    
    printf("Starting data requests (%d iters)\n", usr_par.iters);
    if (gettimeofday(&start, NULL)) {
        perror("gettimeofday");
        ret_val = 1;
        goto clean_rdma_buff;
    }

    /****************************************************************************************************
     * The main loop where client and server send and receive "iters" number of messages
     */
    for (cnt = 0; cnt < usr_par.iters; cnt++) {

        char ackmsg[sizeof "rdma_write completed"];
        int  ret_size;
        
        // Sending RDMA data (address and rkey) by socket as a triger to start RDMA write operation
        DEBUG_LOG_FAST_PATH("Send message N %d: buffer desc \"%s\" of size %d\n", cnt, desc_str, ret_desc_str_size);
        ret_size = write(sockfd, desc_str, ret_desc_str_size);
        if (ret_size != ret_desc_str_size) {
            perror("client write");
            fprintf(stderr, "Couldn't send RDMA data for iteration, write data size %d\n", ret_size);
            ret_val = 1;
            goto clean_rdma_buff;
        }
        
        // Wating for confirmation message from the socket that rdma_write from the server has beed completed
        ret_size = recv(sockfd, ackmsg, sizeof ackmsg, MSG_WAITALL);
        if (ret_size != sizeof ackmsg) {
            perror("client read");
            fprintf(stderr, "Couldn't read \"rdma_write completed\" message, recv data size %d\n", ret_size);
            ret_val = 1;
            goto clean_rdma_buff;
        }

        // Printing received data for debug purpose
        DEBUG_LOG_FAST_PATH("Received ack N %d: \"%s\"\n", cnt, ackmsg);
        if (!usr_par.use_cuda) {
            DEBUG_LOG_FAST_PATH("Written data \"%s\"\n", (char*)buff);
        }
    }
    /****************************************************************************************************/

    ret_val = print_run_time(start, usr_par.size, usr_par.iters);
    if (ret_val) {
        goto clean_rdma_buff;
    }

clean_rdma_buff:
    rdma_buffer_dereg(rdma_buff);

clean_mem_buff:
    work_buffer_free(buff, usr_par.use_cuda);
 
clean_device:
    rdma_close_device(rdma_dev);

clean_socket:
    close(sockfd);

clean_usr_par:
    if (usr_par.bdf) {
        free(usr_par.bdf);
    }

    return ret_val;
}


