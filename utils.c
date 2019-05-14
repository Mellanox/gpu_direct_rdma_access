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
#include <time.h>
#include <netdb.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>

#include "utils.h"

int get_addr(char *dst, struct sockaddr *addr)
{
        struct addrinfo *res;
        int ret;

        ret = getaddrinfo(dst, NULL, NULL, &res);
        if (ret) {
                printf("getaddrinfo failed (%s) - invalid hostname or IP address\n", gai_strerror(ret));
                return ret;
        }

        if (res->ai_family == PF_INET)
                memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in));
        else if (res->ai_family == PF_INET6)
                memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in6));
        else
                ret = -1;

        freeaddrinfo(res);
        return ret;
}

int print_run_time(struct timeval start, unsigned long size, int iters)
{
    struct timeval  end;
    float           usec;
    long long       bytes;

    if (gettimeofday(&end, NULL)) {
        perror("gettimeofday");
        return 1;
    }

    usec  = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
    bytes = (long long) size * iters;

    printf("%lld bytes in %.2f seconds = %.2f Mbit/sec\n",
           bytes, usec / 1000000., bytes * 8. / usec);
    printf("%d iters in %.2f seconds = %.2f usec/iter\n",
           iters, usec / 1000000., usec / iters);
    return 0;

}

