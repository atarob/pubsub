#ifndef __MISC_H
#define __MISC_H

#include "common.h"
#include <sys/types.h>         
#include <sys/socket.h>
#include <netdb.h>

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

static inline
int sscanf_len(const char *s, char *terminator,
               const char *format, ...)
{
    const char tcpy = *terminator;
    *terminator = 0;
    
    va_list ap;
    va_start(ap, format);
    const int rslt = vsscanf(s, format, ap);
    va_end(ap);
    
    *terminator = tcpy;
    
    return rslt;
}

/** Returns true on success, or false if there was an error */
static inline
int set_socket_flag_non_block(int fd, char non_block)
{
    if (fd < 0) return -1;

    int rslt = fcntl(fd, F_GETFL, 0);
    if (rslt < 0) { 
        serv_perror("set_socket_flag_non_block() on fcntl at 1"); 
        return -2;
    }
    uint32_t flags = rslt;
    flags = (non_block ? 
             (flags | (uint32_t)O_NONBLOCK) : 
             (flags &~(uint32_t)O_NONBLOCK));
    
    rslt = fcntl(fd, F_SETFL, flags);
    if(rslt < 0) { 
        serv_perror("set_socket_flag_non_block() on fcntl at 2"); 
        return -3;
    }
    
    return 0;
}

static inline
int set_socket_non_block(int socket)
{
    return set_socket_flag_non_block(socket, 1);
}

static inline int set_socket_recv_timeout(int sock, uint64_t usec)
{
    struct timeval tv;
    BZERO(&tv);
    tv.tv_usec = usec % (1000*1000);
    tv.tv_sec  = usec / (1000*1000);
    int rslt = setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    if(rslt==-1) { 
        serv_perror(FN); serv_error("%s() failed to setsockopt!\n", FN); 
    }
    return rslt;
}

static inline int set_socket_send_timeout(int sock, uint64_t usec)
{
    struct timeval tv;
    BZERO(&tv);
    tv.tv_usec = usec % (1000*1000);
    tv.tv_sec  = usec / (1000*1000);
    int rslt = setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    if(rslt==-1) { 
        serv_perror(FN); serv_error("%s() failed to setsockopt!\n", FN); 
    }
    return rslt;
}

static inline
int set_socket_block(int socket,
                     uint64_t usec //microseconds, 0 means forever
                     )
{
    set_socket_send_timeout(socket, usec);
    return set_socket_flag_non_block(socket, 0);
}

static inline
int make_socket(const char *port,
                const char *host //leave NULL to get listening socket
                )
{
    int sock = -1;
    struct addrinfo hints;
    memset (&hints, 0, sizeof (struct addrinfo));
    hints.ai_family = AF_UNSPEC;       
    hints.ai_socktype = SOCK_STREAM;   
    hints.ai_flags = AI_PASSIVE;       
    
    struct addrinfo * result = NULL;
    if (getaddrinfo(host, port, &hints, &result)) { 
        serv_perror("create_socket() failed getaddrinfo()");
        return -1;
    }
    
    struct addrinfo * rp;
    for(rp = result; rp != NULL; rp = rp->ai_next) {
        sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if(sock == -1) { 
            serv_perror("create_socket() failed socket()");
            continue; 
        }
        int on = 1;
        if(host) {
            if(connect(sock, rp->ai_addr, rp->ai_addrlen) == 0) { break; }
            serv_perror("create_socket() connect() failed");
        } else {
            if(setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
                serv_perror("create_socket() failed setsockopt(...SO_REUSEADDR...)");
            }
            if(bind(sock, rp->ai_addr, rp->ai_addrlen) == 0) { break; }
            serv_perror("create_socket() bind() failed");
        }
        close(sock); sock=-1;
    }
    if(sock!=-1 && !host) {
        if(listen(sock, 128)) {
            serv_perror("create_socket() failed listen()");
            close(sock); sock=-1;
        }
    }
    freeaddrinfo (result);
    return sock;
}

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif
#endif /* __MISC_H */
