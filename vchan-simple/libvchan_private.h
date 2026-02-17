/*
 * The Qubes OS Project, http://www.qubes-os.org
 *
 * Copyright (C) 2020  Paweł Marczewski  <pawel@invisiblethingslab.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 *
 */

#ifndef _LIBVCHAN_PRIVATE_H
#define _LIBVCHAN_PRIVATE_H

#include <stdint.h>
#include <stdbool.h>

#include "libvchan.h"
#include "ring.h"

enum vchan_transport {
    VCHAN_TRANSPORT_UNIX = 0,
    VCHAN_TRANSPORT_VSOCK = 1,
};

struct libvchan {
    char *socket_path;
    int server_fd;
    int socket_fd;
    // distinguish VCHAN_WAITING vs. VCHAN_DISCONNECTED
    bool is_new;
    // blocking mode (default true)
    bool blocking;

    enum vchan_transport transport;
    unsigned int vsock_cid;
    unsigned int vsock_port;

    struct ring read_ring;
    int connect_watch_fd;
};

int libvchan__listen(const char *socket_path);
int libvchan__connect(const char *socket_path);
int libvchan__listen_vsock(unsigned int cid, unsigned int port);
int libvchan__connect_vsock(unsigned int cid, unsigned int port);

#endif
