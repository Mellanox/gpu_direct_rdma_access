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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <dirent.h>

enum ibv_gid_type {
        IBV_GID_TYPE_IB_ROCE_V1,
        IBV_GID_TYPE_ROCE_V2,
};

static int ibv_read_sysfs_file(const char *dir, const char *file,
                        char *buf, size_t size)
{
        char *path;
        int fd;
        int len;

        if (asprintf(&path, "%s/%s", dir, file) < 0)
                return -1;

        fd = open(path, O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
                free(path);
                return -1;
        }

        len = read(fd, buf, size);

        close(fd);
        free(path);

        if (len > 0) {
                if (buf[len - 1] == '\n')
                        buf[--len] = '\0';
                else if (len < size)
                        buf[len] = '\0';
                else
			/* We would have to truncate the contents to NULL
			 * terminate, so we are going to fail no matter
			 * what we do, either right now or later when
			 * we pass around an unterminated string.  Fail now.
			 */
			return -1;
        }

        return len;
}


/* GID types as appear in sysfs, no change is expected as of ABI
 * compatibility.
 */
#define V1_TYPE "IB/RoCE v1"
#define V2_TYPE "RoCE v2"
static int ibv_query_gid_type(struct ibv_context *context, uint8_t port_num,
                unsigned int index, enum ibv_gid_type *type)
{
        char name[32];
        char buff[11];

        snprintf(name, sizeof(name), "ports/%d/gid_attrs/types/%d", port_num,
                        index);

	/* Reset errno so that we can rely on its value upon any error flow in
	 * ibv_read_sysfs_file.
	 */
	errno = 0;
	if (ibv_read_sysfs_file(context->device->ibdev_path, name, buff,
				sizeof(buff)) <= 0) {
		char *dir_path;
		DIR *dir;

		if (errno == EINVAL) {
			/* In IB, this file doesn't exist and the kernel sets
			 * errno to -EINVAL.
			 */
			*type = IBV_GID_TYPE_IB_ROCE_V1;
			return 0;
		}
		if (asprintf(&dir_path, "%s/%s/%d/%s/",
					context->device->ibdev_path, "ports", port_num,
					"gid_attrs") < 0)
			return -1;
		dir = opendir(dir_path);
		free(dir_path);
		if (!dir) {
			if (errno == ENOENT)
				/* Assuming that if gid_attrs doesn't exist,
				 * we have an old kernel and all GIDs are
				 * IB/RoCE v1
				 */
				*type = IBV_GID_TYPE_IB_ROCE_V1;
			else
				return -1;
		} else {
			closedir(dir);
			errno = EFAULT;
			return -1;
		}
	} else {
                if (!strcmp(buff, V1_TYPE)) {
                        *type = IBV_GID_TYPE_IB_ROCE_V1;
                } else if (!strcmp(buff, V2_TYPE)) {
                        *type = IBV_GID_TYPE_ROCE_V2;
                } else {
                        errno = ENOTSUP;
                        return -1;
                }
        }

        return 0;
}

int ibv_find_sgid_type(struct ibv_context *context, uint8_t port_num,
		enum ibv_gid_type gid_type, int gid_family)
{
        enum ibv_gid_type sgid_type = 0;
        union ibv_gid sgid;
        int sgid_family = -1;
        int idx = 0;

        do {
                if (ibv_query_gid(context, port_num, idx, &sgid)) {
                        errno = EFAULT;
                        return -1;
                }
                if (ibv_query_gid_type(context, port_num, idx, &sgid_type)) {
                        errno = EFAULT;
                        return -1;
                }
                if (sgid.raw[0] == 0 && sgid.raw[1] == 0) {
                        sgid_family = AF_INET;
                }

                if (gid_type == sgid_type && gid_family == sgid_family) {
                        return idx;
                }

                idx++;
        } while (gid_type != sgid_type || gid_family != sgid_family);

        return idx;
}


