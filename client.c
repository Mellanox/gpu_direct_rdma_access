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
#include "gpu_direct_rdma_access.h"

extern int debug;
extern int debug_fast_path;

#define DEBUG_LOG if (debug) printf
#define DEBUG_LOG_FAST_PATH if (debug_fast_path) printf
#define FDEBUG_LOG if (debug) fprintf
#define FDEBUG_LOG_FAST_PATH if (debug_fast_path) fprintf

#define ACK_MSG "rdma_task completed"

struct user_params {

    uint32_t  		    task;
    int                     port;
    unsigned long           size;
    int                     iters;
    int                     use_cuda;
    char                   *bdf;
    char                   *servername;
    struct sockaddr         hostaddr;
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
        fprintf(stderr, "FAILURE: %s for %s:%d\n", gai_strerror(ret_val), servername, port);
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
        fprintf(stderr, "FAILURE: Couldn't connect to %s:%d\n", servername, port);
        return -1;
    }

    return sockfd;
}

enum payload_t { RDMA_BUF_DESC, TASK_ATTRS };

struct payload_attr {
	enum payload_t data_t;
	char *payload_str;
};

/************************************************************************************
 * Simple package protocol which packs payload string into allocated memory.
 * Protocol consist of:
 * 		uint8_t payload_t - type of the payload data
 *  	uint16_t payload_size - strlen of the payload_str
 *  	char * payload_str - payload to pack
 * 
 * returns: an integer equal to the size of the copied into package data in bytes
 * _________________________________________________________________________________
 * 
 *            PACKAGE = {|type|size|---------payload----------|}                             
 *                         1b   2b    (size * sizeof(char))b 
 * 
 ***********************************************************************************/
int pack_payload_data(void *package, size_t package_size, struct payload_attr *attr)
{
    uint8_t data_t = attr->data_t;
    uint16_t payload_size = strlen(attr->payload_str) + 1;
    size_t req_size = sizeof(data_t) + sizeof(payload_size) + payload_size * sizeof(char) ;
    if (req_size > package_size) {
        fprintf(stderr, "package size (%lu) is less than required (%lu) for sending payload with attributes\n",
                package_size, req_size);
        return 0;
    }
    memcpy(package, &data_t, sizeof(data_t));
    memcpy(package + sizeof(data_t), &payload_size, sizeof(payload_size));
    memcpy(package + sizeof(data_t) + sizeof(payload_size), attr->payload_str, payload_size * sizeof(char));

    return req_size;
}

//====================================================================================
/*                                           t*/
#define RDMA_TASK_ATTR_DESC_STRING_LENGTH (sizeof "12345678")
/*************************************************************************************
 * Get a rdma_task_attr_flags description string representation
 *
 * The Client application should pass this description string to the
 * Server which will issue the RDMA Read/Write operation
 *
 * desc_str is input and output holding the rdma_task_attr_flags information
 * desc_length is input size in bytes of desc_str
 *
 * returns: an integer equal to the size of the char data copied into desc_str
 ************************************************************************************/
int rdma_task_attr_flags_get_desc_str(uint32_t flags, char *desc_str, size_t desc_length)
{
    if (desc_length < RDMA_TASK_ATTR_DESC_STRING_LENGTH) {
        fprintf(stderr, "desc string size (%lu) is less than required (%lu) for sending rdma_task_attr_flags data\n",
                desc_length, RDMA_TASK_ATTR_DESC_STRING_LENGTH);
        return 0;
    }
   
    sprintf(desc_str, "%08x", flags);
    
    return strlen(desc_str) + 1; /*including the terminating null character*/
}

static void usage(const char *argv0)
{
    printf("Usage:\n");
    printf("  %s <host>     connect to server at <host>\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -t, --task_flags=<flags>  rdma task attrs bitmask: bit 0 - rdma operation type: 0 - \"WRITE\"(default),\n"
           "                                                                                  1 - \"READ\"\n");
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
    usr_par->task       = 0;

    while (1) {
        int c;

        static struct option long_options[] = {
            { .name = "task-flags",    .has_arg = 1, .val = 't' },
            { .name = "addr",          .has_arg = 1, .val = 'a' },
            { .name = "port",          .has_arg = 1, .val = 'p' },
            { .name = "size",          .has_arg = 1, .val = 's' },
            { .name = "iters",         .has_arg = 1, .val = 'n' },
            { .name = "use-cuda",      .has_arg = 1, .val = 'u' },
            { .name = "debug-mask",    .has_arg = 1, .val = 'D' },
            { 0 }
        };

        c = getopt_long(argc, argv, "t:a:p:s:n:u:D:",
                        long_options, NULL);
        if (c == -1)
            break;

        switch (c) {
        
        case 't':
            usr_par->task = (strtol(optarg, NULL, 0) >> 0) & 1; /*bit 0*/
            break;

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
                fprintf(stderr, "FAILURE: BDF mem alloc failure (errno=%d '%m')", errno);
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
        fprintf(stderr, "FAILURE: Server name is missing in the commant line.\n");
        usage(argv[0]);
        return 1;
    } else if (optind == argc - 1) {
        //usr_par->servername = strdupa(argv[optind]);
        usr_par->servername = calloc(1, strlen(argv[optind])+1);
        if (!usr_par->servername){
            fprintf(stderr, "FAILURE: servername mem alloc failure (errno=%d '%m')", errno);
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
        fprintf(stderr, "FAILURE: host ip address is missing in the command line.");
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
    rdma_dev = rdma_open_device_client(&usr_par.hostaddr);

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

    char desc_str[256], task_opt_str[16];

    int ret_desc_str_size = rdma_buffer_get_desc_str(rdma_buff, desc_str, sizeof(desc_str));
    int ret_task_opt_str_size = rdma_task_attr_flags_get_desc_str(usr_par.task, task_opt_str, sizeof(task_opt_str));
     
    if (!ret_desc_str_size || !ret_task_opt_str_size) {
        ret_val = 1;
        goto clean_rdma_buff;
    }

    /* Package memory allocation */
    const int package_size = (ret_desc_str_size + ret_task_opt_str_size) * sizeof(char) + 2 * sizeof(uint16_t) + 2 * sizeof(uint8_t);
    void *package = malloc(package_size);
    memset(package, 0, package_size);

    /* Packing RDMA buff desc str */
    struct payload_attr pl_attr = { .data_t = RDMA_BUF_DESC, .payload_str = desc_str };
    int buff_package_size = pack_payload_data(package, package_size, &pl_attr);
    if (!buff_package_size) {
        ret_val = 1;
        goto clean_package_data;
    }
    
    /* Packing RDMA task attrs desc str */
    pl_attr.data_t = TASK_ATTRS;
    pl_attr.payload_str = task_opt_str;
    buff_package_size += pack_payload_data(package + buff_package_size, package_size, &pl_attr);
     if (!buff_package_size) {
        ret_val = 1;
        goto clean_package_data;
    }
    
    printf("Starting data transfer (%d iters)\n", usr_par.iters);
    if (gettimeofday(&start, NULL)) {
        fprintf(stderr, "FAILURE: gettimeofday (errno=%d '%m')", errno);
        ret_val = 1;
        goto clean_package_data;
    }

    /****************************************************************************************************
     * The main loop where client and server send and receive "iters" number of messages
     */
    for (cnt = 0; cnt < usr_par.iters; cnt++) {

        char ackmsg[sizeof ACK_MSG];
        int  ret_size;
        
        // Sending RDMA data (address and rkey) by socket as a triger to start RDMA read/write operation
        DEBUG_LOG_FAST_PATH("Send message N %d: buffer desc \"%s\" of size %d with task opt \"%s\" of size %d\n", cnt, desc_str, strlen(desc_str), task_opt_str, strlen(task_opt_str));
        ret_size = write(sockfd, package, buff_package_size);
        if (ret_size != buff_package_size) {
            fprintf(stderr, "FAILURE: Couldn't send RDMA data for iteration, write data size %d (errno=%d '%m')\n", ret_size, errno);
            ret_val = 1;
            goto clean_package_data;
        }
        
        // Wating for confirmation message from the socket that rdma_read/write from the server has beed completed
        ret_size = recv(sockfd, ackmsg, sizeof ackmsg, MSG_WAITALL);
        if (ret_size != sizeof ackmsg) {
            fprintf(stderr, "FAILURE: Couldn't read \"%s\" message, recv data size %d (errno=%d '%m')\n", ACK_MSG, ret_size, errno);
            ret_val = 1;
            goto clean_package_data;
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
        goto clean_package_data;
    }

clean_package_data:
    free(package);

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
