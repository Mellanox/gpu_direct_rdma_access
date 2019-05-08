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
#define FDEBUG_LOG_FAST_PATH if (debug_fast_path) sprintf
#define SDEBUG_LOG if (debug) fprintf
#define SDEBUG_LOG_FAST_PATH if (debug_fast_path) sprintf

struct user_params {

    int                  port;
    unsigned long        size;
    int                  iters;
    struct sockaddr      hostaddr;
};

/****************************************************************************************
 * Open temporary socket connection on the server side, listening to the client.
 * Accepting connection from the client and closing temporary socket.
 * If success, return the accepted socket file descriptor ID
 * Return value: socket fd - success, -1 - error
 ****************************************************************************************/
static int open_server_socket(int port)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = {
        .ai_flags    = AI_PASSIVE,
        .ai_family   = AF_UNSPEC,
        .ai_socktype = SOCK_STREAM
    };
    char   *service;
    int     ret_val;
    int     sockfd;
    int     tmp_sockfd = -1;

    ret_val = asprintf(&service, "%d", port);
    if (ret_val < 0)
        return -1;

    ret_val = getaddrinfo(NULL, service, &hints, &res);
    if (ret_val < 0) {
        fprintf(stderr, "%s for port %d\n", gai_strerror(ret_val), port);
        free(service);
        return -1;
    }

    for (t = res; t; t = t->ai_next) {
        tmp_sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (tmp_sockfd >= 0) {
            int optval = 1;

            setsockopt(tmp_sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof optval);

            if (!bind(tmp_sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(tmp_sockfd);
            tmp_sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (tmp_sockfd < 0) {
        fprintf(stderr, "Couldn't listen to port %d\n", port);
        return -1;
    }

    listen(tmp_sockfd, 1);
    sockfd = accept(tmp_sockfd, NULL, 0);
    close(tmp_sockfd);
    if (sockfd < 0) {
        fprintf(stderr, "accept() failed\n");
        return -1;
    } 
    
    return sockfd;
}

static void usage(const char *argv0)
{
    printf("Usage:\n");
    printf("  %s            start a server and wait for connection\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -a, --addr=<ipaddr>       ip address of the local host net device <ipaddr v4> (mandatory)\n");
    printf("  -p, --port=<port>         listen on/connect to port <port> (default 18515)\n");
    printf("  -s, --size=<size>         size of message to exchange (default 4096)\n");
    printf("  -n, --iters=<iters>       number of exchanges (default 1000)\n");
    printf("  -D, --debug-mask=<mask>   debug bitmask: bit 0 - debug print enable,"
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
            { .name = "debug-mask",    .has_arg = 1, .val = 'D' },
            { 0 }
        };

        c = getopt_long(argc, argv, "a:p:s:m:n:D:",
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

        case 'n':
            usr_par->iters = strtol(optarg, NULL, 0);
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

    if (optind < argc) {
        usage(argv[0]);
        return 1;
    }

    return 0;
}

int main(int argc, char *argv[])
{
    struct rdma_device     *rdma_dev;
    struct timeval          start;
    int                     scnt = 0;
    struct user_params      usr_par;
    int                     ret_val = 0;
    int                     sockfd;
    void                   *buff;

    srand48(getpid() * time(NULL));

    ret_val = parse_command_line(argc, argv, &usr_par);
    if (ret_val) {
        return ret_val;
    }

    rdma_dev = rdma_open_device_source(&usr_par.hostaddr); /* server */
    if (!rdma_dev) {
        ret_val = 1;
        goto clean_socket;
    }
    
    /* Local memory buffer allocation */
    buff = work_buffer_alloc(usr_par.size, 0 /*use_cuda*/);
            /* On the server side, we allocate buffer on CPU and not on GPU */
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

    printf("Listening to remote client...\n");
    sockfd = open_server_socket(usr_par.port);
    if (sockfd < 0) {
        return 1;
    }
    printf("Connection accepted.\n");

    if (gettimeofday(&start, NULL)) {
        perror("gettimeofday");
        ret_val = 1;
        goto clean_rdma_buff;
    }
 
    /****************************************************************************************************
     * The main loop where we client and server send and receive "iters" number of messages
     */
    scnt = 0;
    while (scnt < usr_par.iters) {

        int  r_size;
        char desc_str[sizeof "0102030405060708:01020304:01020304:0102:010203:1:0102030405060708090a0b0c0d0e0f10"];
        char ackmsg[sizeof "rdma_write completed"];
        struct rdma_write_attr  write_attr;

        /* Receiving RDMA data (address, size, rkey etc.) from socket as a triger to start RDMA write operation */
        DEBUG_LOG_FAST_PATH("Iteration %d: Waiting to Receive message of size %d\n", scnt, sizeof desc_str);
        r_size = recv(sockfd, desc_str, sizeof desc_str, MSG_WAITALL);
        if (r_size != sizeof desc_str) {
            fprintf(stderr, "Couldn't receive RDMA data for iteration %d\n", scnt);
            ret_val = 1;
            goto clean_rdma_buff;
        }
        DEBUG_LOG_FAST_PATH("Received message \"%s\"\n", desc_str);
        memset(&write_attr, 0, sizeof write_attr);
        write_attr.remote_buf_desc_str      = desc_str;
        write_attr.remote_buf_desc_length   = sizeof desc_str;
        write_attr.local_buf_rdma           = rdma_buff;
        write_attr.wr_id                    = scnt;

        /* Executing RDMA write */
        SDEBUG_LOG_FAST_PATH ((char*)buff, "Write iteration N %d", scnt);
        ret_val = rdma_write_to_peer(&write_attr);
        if (ret_val) {
            goto clean_rdma_buff;
        }
        
        /* Completion queue polling loop */
        struct rdma_completion_event rdma_comp_ev[10];
        int    reported_ev, i;
        
        DEBUG_LOG_FAST_PATH("Polling completion queue\n");
        do {
            reported_ev = rdma_poll_completions(rdma_dev, rdma_comp_ev, 10);
            //TODO - we can put sleep here
        } while (reported_ev < 1);
        DEBUG_LOG_FAST_PATH("Finished polling\n");

        for (i = 0; i < reported_ev; ++i) {
            if (rdma_comp_ev[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr, "Failed status \"%s\" (%d) for wr_id %d\n",
                        ibv_wc_status_str(rdma_comp_ev[i].status),
                        rdma_comp_ev[i].status, (int) rdma_comp_ev[i].wr_id);
                ret_val = 1;
                goto clean_rdma_buff;
            }
        }
        scnt += reported_ev;

        // Sending ack-message to the client, confirming that RDMA write has been completet
        if (write(sockfd, "rdma_write completed", sizeof("rdma_write completed")) != sizeof("rdma_write completed")) {
            fprintf(stderr, "Couldn't send \"rdma_write completed\" msg\n");
            return 1;
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
    work_buffer_free(buff, 0);

clean_device:
    rdma_close_device(rdma_dev);

clean_socket:
    close(sockfd);

    return ret_val;
}
