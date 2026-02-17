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
#include <pthread.h>

#include "libvchan.h"
#include "ring.h"

enum vchan_transport {
    VCHAN_TRANSPORT_UNIX = 0,
    VCHAN_TRANSPORT_VSOCK = 1,
};

struct libvchan {
    char *socket_path;
    // server socket (for server), connection (for client)
    int socket_fd;

    enum vchan_transport transport;
    /* vsock parameters (used when transport == VCHAN_TRANSPORT_VSOCK) */
    unsigned int vsock_cid;
    unsigned int vsock_port;

    // Controls access to rings and state
    pthread_mutex_t mutex;

    pthread_t thread;

    // Thread started
    volatile int thread_started;

    // Thread exiting / exited
    volatile int shutdown;

    // For libvchan_is_open
    volatile int state;

    // Notification about changes in ring (data added/removed) from user thread
    int user_event_pipe[2];

    // Notification about changes in ring (data added/removed) and connection
    // status
    int socket_event_pipe[2];

    // volatile EVTCHN state;
    struct ring read_ring;
    struct ring write_ring;

    // used for cleanup after libvchan_client_init_async()
    int connect_watch_fd;

    // blocking mode: if true, reads/writes block until data is available/space
    // is free (default). If false, return immediately with partial results.
    volatile int blocking;
};

void *libvchan__server(void *arg);
void *libvchan__client(void *arg);
int libvchan__drain_pipe(int fd);
int libvchan__listen(const char *socket_path);
int libvchan__connect(const char *socket_path);
int libvchan__listen_vsock(unsigned int cid, unsigned int port);
int libvchan__connect_vsock(unsigned int cid, unsigned int port);

#endif
