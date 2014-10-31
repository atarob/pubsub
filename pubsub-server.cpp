#include "common.h" 
#include "usersem.h" 
#include "varmap.h"
#include "pubsub-engine.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h> 
#include <sys/epoll.h>
#include <pthread.h>  // build with -pthread option, should switch to clone at some point.
#include <poll.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/sendfile.h>
#include <time.h>
#include <signal.h>
#include <assert.h>
#include <set>
#include <string>
#include <unordered_map>

using namespace std;

#define PIPE_READ 0
#define PIPE_WRITE 1
struct server {
    const char *listen_port;
    int listen_sock;
    int epoll_fd;
    int crypto_fd;
    int main_thread_pipe[2];
    pthread_t main_thread;
    struct semaphore epoll_wait_threads;
    struct semaphore socket_read_buf_lock[N_ROT_LOCKS];
    struct semaphore recv_queue_lock;
    struct semaphore service_thread_factory_lock;
    struct semaphore service_threads_sem;
    struct semaphore host_socket_lock;
    unsigned long local_ip; 
    char term_requested;
    unordered_map<int, vector<char> * > recv_queue;
    unordered_map<string, int> host_socket;
    unordered_map<int, string> socket_host;
    unordered_multimap<int, uint32_t> upstreams;
    struct engine engine;
};

static inline int trylock_recv_queue(struct server *server)
{
    return futex_trydown(&server->recv_queue_lock);
}

static inline void lock_recv_queue(struct server *server)
{
    while(futex_down(&server->recv_queue_lock)) { 
        serv_perror(FN); 
    }
}

static inline void unlock_recv_queue(struct server *server)
{
    futex_up(&server->recv_queue_lock);
}

struct socket_meta {
    uint32_t is_upstream:1 __attribute__((packed));
    uint32_t is_local:1 __attribute__((packed));
};

struct epoll_u64_parts {
    int32_t socket;
    struct socket_meta meta;
};

union epoll_u64 {
    uint64_t val;
    struct epoll_u64_parts parts;
};

static inline 
int epoll_socket(struct server *server, 
                 const int socket, 
                 const int op,
                 const int event,
                 const int is_upstream,
                 const int is_local)
{
    const char *op_name = "UNKNOWN";
    if(op==EPOLL_CTL_ADD) { op_name = "EPOLL_CTL_ADD"; }
    else if(op==EPOLL_CTL_MOD) { op_name = "EPOLL_CTL_MOD"; }
    else if(op==EPOLL_CTL_DEL) { op_name = "EPOLL_CTL_DEL"; }
    
    if(socket == server->listen_sock) {
        dbg_printf(FN, LN, "listen_socket:%s", op_name);
    } else if(socket == server->main_thread_pipe[0]) {
        dbg_printf(FN, LN, "main_thread_pipe(read):%s", op_name);
    } else if(socket == server->main_thread_pipe[1]) {
        dbg_printf(FN, LN, "main_thread_pipe(write):%s", op_name);
    } else {
        dbg_printf(FN, LN, "sock:%d, %s, upstream:%d, local:%d", 
                   socket, op_name, is_upstream, is_local);
    }
    
    union epoll_u64 u64 = 
    { parts:{ socket:socket, 
        meta:{is_upstream:is_upstream, is_local:is_local} } };
    struct epoll_event evnt = 
    { events:event, data : { u64:u64.val } };
    if(epoll_ctl(server->epoll_fd, op, socket, &evnt)) {
        serv_perror("epoll_socket() failed on epoll_ctl()");
        return -1; 
    }
    
    return 0;
}

static inline
int epoll_socket_create(struct server *server, 
                        const int socket,
                        const int is_upstream,
                        const int is_local)
{
    return epoll_socket(server, socket, EPOLL_CTL_ADD, 
                        EPOLLRDHUP|EPOLLIN|EPOLLONESHOT, 
                        is_upstream, is_local);
}

static inline
int epoll_socket_rearm(struct server *server, 
                       const int socket,
                       const int is_upstream,
                       const int is_local)
{
    dbg_printf(FN, LN, "sock:%d, is_upstream:%d, is_local:%d",
                 socket, is_upstream, is_local);
    return epoll_socket(server, socket, EPOLL_CTL_MOD, 
                        EPOLLIN|EPOLLONESHOT, is_upstream, is_local);
}

static inline
int epoll_socket_destroy(struct server *server, 
                         const int socket)
{
    dbg_printf(FN, LN, "sock:%d", socket);
    return epoll_socket(server, socket, EPOLL_CTL_DEL, 0, 0, 0);
}

static inline
int start_listen(struct server *server)
{
    server->listen_sock = make_socket(server->listen_port, NULL);
    if(server->listen_sock==-1) { return -1; }
    
    set_socket_non_block(server->listen_sock);
    
    if(epoll_socket_create(server, server->listen_sock, 0, 0)) { 
        close(server->listen_sock);
        server->listen_sock = -1;
        return -3;
    }
    
    return 0;
}

static inline
void make_socket_recv_queue(struct server *server,
                            const int socket)
{
    lock_recv_queue(server);
    server->recv_queue[socket] = new vector<char>;
    unlock_recv_queue(server);
}

void destroy_socket_recv_queue(struct server *server,
                               const int socket)
{
    vector<char> *v = NULL;
    lock_recv_queue(server);
    auto it = server->recv_queue.find(socket);
    if(it!=server->recv_queue.end()) {
        v = it->second;
        server->recv_queue.erase(it);
    }
    unlock_recv_queue(server);
    if(v) { delete v; v = NULL; }
}

static inline
int create_downstream(struct server *server,
                      const int listen_sock)
{
    //serv_out("event bits: %d\n",Found.events);
    sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);
    
    int sock = accept4(listen_sock, (sockaddr *)&addr, &addrlen, 
                       SOCK_NONBLOCK);
    if(sock==-1) { 
        serv_perror("prep_connect() failed on accept()"); return -1; 
    }
    if(epoll_socket_rearm(server, server->listen_sock, 0, 0)) { return -1;}
    //some sort of authentication?
    make_socket_recv_queue(server, sock);
    int rslt = epoll_socket_create(server, sock, 0,
                                   (addr.sin_addr.s_addr==server->local_ip)?1:0);
    if(rslt) { return -1; }
    
    return sock;
}

//call get_host_socket() instead of this directly
int create_upstream(struct server *server,
                    const char *host,
                    const char *port)
{
    int socket = make_socket(port, host);
    if(socket==-1) { return -1; }
    
    set_socket_non_block(socket);
    make_socket_recv_queue(server, socket);
    int rslt = epoll_socket_create(server, socket, 1, 0);
    if(rslt) { return -1; }
    
    server->host_socket.insert(pair<string, int>(host, socket));
    server->socket_host.insert(pair<int, string>(socket, host));
    
    return socket;
}

static inline
int get_host_socket(struct server *server,
                    const char *host,
                    const char *port)
{
    int socket = -1;
    while(futex_down(&server->host_socket_lock)) { serv_perror(FN); }
    
    auto it = server->host_socket.find(host);
    if(it != server->host_socket.end()) { socket = it->second; }
    else { socket = create_upstream(server, host, port); }
    
    futex_up(&server->host_socket_lock);
    return socket;
}

const char cmd_term[] = { 13, 10 };
const size_t cmd_term_len = sizeof(cmd_term);

static inline
int stream_publish(struct engine *engine,
                   class vector<char> *stack,
                   const uint32_t stream_id,
                   const void *cmd, //assumes it followed by cmd_term
                   const size_t cmd_len)
{
    struct stream *s = &engine->streams[stream_id];
    int rslt = 0;
    
    const int master_sock = stream_master_socket(s);
    if(master_sock > 0) { //forward to master
        dbg_printf(FN,LN, "cmd:");
        write(2, cmd, cmd_len + cmd_term_len);
        dbg_printf(FN,LN, "|\n");
        noblock_send(engine, master_sock, cmd, cmd_len + cmd_term_len);
        return rslt;
    }
    //master_sock must be 0 (master) or -1 (neither master nor subscribed)
    const char * buf = (const char *)memchr(cmd, '|', cmd_len);
    if(buf) { 
        buf++; 
        const size_t len = cmd_len - (buf - (const char *)cmd);
        const size_t sp = stack->size();
        message_create(stack, stream_id, buf, len);
        rslt = stream_write(engine, &(*stack)[sp], stack->size()-sp, -1);
        stack->resize(sp);
    }
    
    return rslt;
}
                           
int stream_unslave(struct server *server,
                   vector<char> *stack,
                   const uint32_t stream_id)
{
    struct stream *s = &server->engine.streams[stream_id];
    write_lock(&s->map_lock);
    int host_socket = s->master_socket;
    s->master_socket = -1;
    futex_down2(&server->host_socket_lock, FN);
    const auto range = server->upstreams.equal_range(host_socket);
    for(auto it = range.first; it!=range.second; it++) {
        if(it->second == stream_id) { server->upstreams.erase(it); }
    }
    futex_up(&server->host_socket_lock);
    
    if(host_socket == -1) { write_unlock(&s->map_lock); return 1; }
    
    const size_t sp = stack->size();
    
    cvector_sprintf(stack, "unsub %x", stream_id);
    cvector_append(stack, cmd_term, cmd_term_len);
    noblock_send(&server->engine, host_socket, &stack[sp], stack->size()-sp);
    
    write_unlock(&s->map_lock);
    stack->resize(sp);
    
    return 0;
}

void host_socket_unslave(struct server *server,
                         vector<char> *stack,
                         const int socket)
{
    futex_down2(&server->host_socket_lock, FN);
    do { 
        const auto it = server->upstreams.find(socket);
        if(it == server->upstreams.end()) { break; }
        
        futex_up(&server->host_socket_lock);
        stream_unslave(server, stack, it->second);
        futex_down2(&server->host_socket_lock, FN);
    } while(1);
    futex_up(&server->host_socket_lock);
}

static inline
int del_host_socket(struct server *server,
                    vector<char> *stack,
                    const int socket)
{
    int rslt = 1;
    futex_down2(&server->host_socket_lock, FN);
    
    auto it = server->socket_host.find(socket);
    if(it != server->socket_host.end()) { 
        server->host_socket.erase(it->second);
        server->socket_host.erase(it);
        rslt = 0;
    }
    
    futex_up(&server->host_socket_lock);
    host_socket_unslave(server, stack, socket);
    return rslt;
}

void socket_close(struct server *server,
                  vector<char> *stack,
                  const int socket)
{
    epoll_socket_destroy(server, socket);
    destroy_socket_recv_queue(server, socket);
    
    socket_unsubscribe(&server->engine, socket);
    del_host_socket(server, stack, socket);
    
    lock_socket(&server->engine, socket);
    lock_socket_buf(&server->engine, socket);
    socket_flush(&server->engine, socket);
    destroy_socket_send_queue(&server->engine, socket);    
    unlock_socket(&server->engine, socket);
    unlock_socket_buf(&server->engine, socket); 
    close(socket);
}

//returns 0 for not-final, and 1 for final packet (EOF) input
int buffer_recv(struct server *server, 
                const int socket)
{
    int final = 0;
    //remember the socket is not rearmed in epoll, so we have effectively
    //  a lock on the socket's receive side
    vector<char> *v = NULL;
    lock_recv_queue(server);
    const auto it = server->recv_queue.find(socket);
    if(it!=server->recv_queue.end()) { v = it->second; }
    unlock_recv_queue(server);
    if(!v) { return 1; }
    
    size_t bytes = 1<<16;
    ssize_t rslt;
    while(bytes) {
        const size_t offst = v->size();
        v->resize(offst + bytes);
        rslt = read(socket, &(*v)[offst], bytes);
        if(rslt==0) { final = 1; }
        if(rslt != (ssize_t) bytes) { break; }
        bytes <<= 1;
    }
    if(!bytes) { serv_segfault(FN, "size_t shouldn't overflow!"); }
    
    v->resize(v->size()-bytes+MAX(rslt,0));
    return final;
}

static inline
vector<char> *socket_recv_queue(struct server *server, 
                                const int socket)
{
    vector<char> *v = NULL;
    lock_recv_queue(server);
    auto it = server->recv_queue.find(socket);
    if(it!=server->recv_queue.end()) { v = it->second; }
    unlock_recv_queue(server);
    
    return v;
}

static inline
int process_messages(struct server *server, 
                     const int socket)
{
    vector<char> *v = socket_recv_queue(server, socket);
    if(!v) { return -1; }
    
    size_t offst = 0;
    const size_t sz = v->size();
    while(1) {
        struct msg_header *hdr = (struct msg_header *)&(*v)[offst];
        if(sz-offst < sizeof(*hdr)) { break; }
        const size_t bytes = offsetof(typeof(*hdr), buf) + hdr->len;
        if(sz-offst < bytes) { break; }
        stream_write(&server->engine, &(*v)[offst], bytes, socket);
        
        offst += bytes;
    } 
    
    if(offst) {
        memmove(&(*v)[0], &(*v)[offst], sz-offst);
        v->resize(sz-offst);
    }
    
    return 0;
}

#define CMD_LEN 64
#define HOST_LEN 512
#define PORT_LEN 12

static inline
int cmd_publish(struct server *server,
                vector<char> *stack,
                const char *current, 
                char *term)
{
    char cmd[CMD_LEN+1];
    uint32_t stream_id;
    int rslt =
          sscanf_len(current, term, "%"STRINGIZE_VAL(CMD_LEN)"s"
                    " %x", //stream
                    cmd, &stream_id);
    if(rslt!=2) { 
        dbg_printf(FN, LN, "rslt:%d, cmd:%s", rslt, cmd);
        return 1; 
    }
    
    dbg_printf(FN, LN, "cmd:%s stream_id:%x", cmd, stream_id);
    return stream_publish(&server->engine, stack, stream_id, current, 
                          term-current);
}

static inline
int cmd_subscribe(struct server *server,
                          const int socket,
                          const char *current, 
                          char *term)
{
    char cmd[CMD_LEN+1];
    uint32_t stream_id;
    uint64_t catchup_from;
    int rslt =
          sscanf_len(current, term, "%"STRINGIZE_VAL(CMD_LEN)"s"
                    " %x" //stream
                    " %llu", //catchup_from timestamp
                    cmd, &stream_id, &catchup_from);
    if(rslt!=3) { return 1; }
    
    dbg_printf(FN, LN, "cmd:%s stream_id:%x, catchup_from:%llu", 
               cmd, stream_id, catchup_from);
    return stream_subscribe(&server->engine, stream_id, socket, 
                            catchup_from);
}

int stream_slave(struct server *server,
                 vector<char> *stack,
                 const char *host,
                 const char *port,
                 const uint32_t stream_id,
                 uint64_t catchup_from)
{
    const int host_socket = get_host_socket(server, host, port);
    if(host_socket==-1) { return 2; }
    
    int rslt = 0;
    struct stream *s = &server->engine.streams[stream_id];
    write_lock(&s->map_lock);
    s->is_master = 0;
    s->master_socket = host_socket;
    futex_down2(&server->host_socket_lock, FN);
    server->upstreams.insert(pair<int, uint32_t>(host_socket, stream_id));
    futex_up(&server->host_socket_lock);
    uint64_t timestamp=0;
    if(s->indx->len) {
        timestamp = VARMAP_ARR_2(s->indx, s->indx->len, 
                                 struct stream_indx_entry)[-1].timestamp;
        timestamp++; //dont duplicate, but possibly miss identical t record
    }
    
    catchup_from = MAX(catchup_from, timestamp);
    const size_t sp = stack->size();
    cvector_sprintf(stack, "sub %x %llu", stream_id, 
                    (unsigned long long)catchup_from);
    cvector_append(stack, cmd_term, cmd_term_len);
    noblock_send(&server->engine, host_socket, &(*stack)[sp], stack->size()-sp);
    
    write_unlock(&s->map_lock);
    stack->resize(sp);
    
    return rslt;
}

static inline
int cmd_slave(struct server *server,
              vector<char> *stack,
              const char *current, 
              char *term)
{
    char cmd[CMD_LEN+1];
    char host[HOST_LEN+1];
    char port[PORT_LEN+1];
    uint32_t stream_id;
    uint64_t catchup_from;
    int rslt =
          sscanf_len(current, term, "%"STRINGIZE_VAL(CMD_LEN)"s" 
                    " %"STRINGIZE_VAL(HOST_LEN)"s"
                    " %"STRINGIZE_VAL(PORT_LEN)"s"
                    " %x" //stream
                    " %llu", //catchup_from timestamp
                    cmd, host, port, &stream_id, &catchup_from);
    if(rslt!=5) { return 1; }
    
    return stream_slave(server, stack, 
                        host, port, stream_id, catchup_from);
}

static inline
int cmd_unslave(struct server *server,
                vector<char> *stack,
                const char *current, 
                char *term)
{
    char cmd[CMD_LEN+1];
    uint32_t stream_id;
    int rslt =
          sscanf_len(current, term, "%"STRINGIZE_VAL(CMD_LEN)"s" 
                    " %x", //stream
                    cmd, &stream_id);
    if(rslt!=2) { return 1; }
    
    return stream_unslave(server, stack, stream_id);
}

void stream_master(struct server *server, 
                   vector<char> *stack,
                   const uint32_t stream_id)
{
    struct stream *s = &server->engine.streams[stream_id];
    write_lock(&s->map_lock);
    while(s->master_socket!=-1) {
        write_unlock(&s->map_lock);
        stream_unslave(server, stack, stream_id);
        write_lock(&s->map_lock);
    }
    s->is_master = 1;
    write_unlock(&s->map_lock);
}

static inline
int cmd_master(struct server *server,
               vector<char> *stack,
               const char *current, 
               char *term)
{
    char cmd[CMD_LEN+1];
    uint32_t stream_id;
    int rslt =
          sscanf_len(current, term, "%"STRINGIZE_VAL(CMD_LEN)"s"
                    " %x", //stream
                    cmd, &stream_id);
    if(rslt!=2) { return 1; }
    
    dbg_printf(FN, LN, "cmd:%s stream_id:%x", cmd, stream_id);

    stream_master(server, stack, stream_id);
    return 0;
}

void stream_unmaster(struct server *server, 
                     const uint32_t stream_id)
{
    struct stream *s = &server->engine.streams[stream_id];
    write_lock(&s->map_lock);
    s->is_master = 0;
    write_unlock(&s->map_lock);
}

static inline
int cmd_unmaster(struct server *server,
                 const char *current, 
                 char *term)
{
    char cmd[CMD_LEN+1];
    uint32_t stream_id;
    int rslt =
          sscanf_len(current, term, "%"STRINGIZE_VAL(CMD_LEN)"s"
                    " %x", //stream
                    cmd, &stream_id);
    if(rslt!=2) { return 1; }
    
    stream_unmaster(server, stream_id);
    return 0;
}

static inline
int process_command(struct server *server,
                    vector<char> *stack,
                    const int socket,
                    const char *current, 
                    char *term,
                    const int is_local)
{
    char cmd[CMD_LEN+1];
    int rslt = 
          sscanf_len(current, term, "%"STRINGIZE_VAL(CMD_LEN)"s", cmd);
    if(rslt!=1) { return 1; }
    
    dbg_printf(FN, LN, "cmd:%s", cmd);
    if(!strcmp(cmd, "close")) { return -1; }
    else if(!strcmp(cmd, "pub")) { 
        cmd_publish(server, stack, current, term);
    }
    else if(!strcmp(cmd, "sub")) { 
        return cmd_subscribe(server, socket, current, term);
    }
    else if(!is_local) { return 1; } //only allow local admin
    
    else if(!strcmp(cmd, "slave")) { 
        cmd_slave(server, stack, current, term);
    }
    if(!strcmp(cmd, "unslave")) { 
        cmd_unslave(server, stack, current, term);
    }
    if(!strcmp(cmd, "master")) { 
        cmd_master(server, stack, current, term);
    }
    if(!strcmp(cmd, "unmaster")) { 
        cmd_unmaster(server, current, term);
    }
    if(!strcmp(cmd, "quit")) { 
        pthread_kill(server->main_thread, SIGINT);
    }
#ifdef DEBUG__MODE
    if(!strcmp(cmd, "hang")) { 
        while(1) {} 
    }
#endif
    
    return 0; //command not found
}

static inline
int process_commands(struct server *server,
                     class vector<char> *stack,
                     const int socket,
                     const int is_local)
{
    vector<char> *v = socket_recv_queue(server, socket);
    if(!v) { return -1; }
    
#define V (*v)
    size_t current = 0;
    const size_t sz = v->size();
    int rslt = 0;
    while(rslt>=0 && current<sz) {
        char *next_term = (char *)memmem(&V[current], sz-current, 
                                         cmd_term, cmd_term_len);
        if(!next_term) { break; }
        
        const size_t bytes = next_term-&V[current];
        
        rslt = process_command(server, stack, socket, &V[current], 
                               next_term, is_local);
        current += bytes + cmd_term_len;//jump the terminator
    } 
    
    if(current) {
        if(current<sz) { memmove(&V[0], &V[current], sz-current); }
        v->resize(sz-current);
    }
    
    if(rslt<0) { return -1; }
    
    return 0;
#undef V
}

#define MAX_STR_ERR 128

int spawn_service_thread(struct server *server);

static inline 
int service_thread_wait(struct server *server,
                        struct epoll_event *found,
                        const char first)
{
    futex_up(&server->epoll_wait_threads); 
    if(first) { futex_up(&server->service_thread_factory_lock); }
    const int eprslt = epoll_wait(server->epoll_fd, found, 1 , -1);
    int err = errno;
    while(futex_down(&server->epoll_wait_threads)) { serv_perror(FN);} 

    if(eprslt != 1) {
        char err_buf[MAX_STR_ERR+1];
        err_buf[MAX_STR_ERR] = 0; //extra terminator in case strerror messes up, not likely!
        strerror_r(err, err_buf, MAX_STR_ERR);
        serv_error("%s() on epoll_wait() returned: %d (strerror=> %s)\n",
                   FN, err, err_buf);  
        return -1;
    }
    if(server->epoll_wait_threads.resource==0) {
        spawn_service_thread(server);
    }
    return 0;
}

int no_sigs()
{
    sigset_t mask;
    sigfillset(&mask);
    if(pthread_sigmask(SIG_SETMASK, &mask, NULL)) {
        serv_perror("no_sigs() failed on pthread_sigmask()");
        return -1;
    }
    return 0;
}

int setup_main_thread_pipe(struct server *server)
{
    if(pipe2(server->main_thread_pipe , 0// O_NONBLOCK
             )) {
        serv_perror("setup_main_thread_pipe() failed on pipe()");
        return -1; 
    }
    epoll_socket_create(server, server->main_thread_pipe[PIPE_READ], 0, 0);
    return 0;
}

static inline
int socket_handle_errors(struct server *server,
                         vector<char> *stack,
                         struct epoll_event *evnt)
{
    if(!(evnt->events & (EPOLLRDHUP|EPOLLERR|EPOLLHUP))) { return 0; }
    
    const union epoll_u64 u64 = { val:evnt->data.u64 };
    
    if(u64.parts.socket==server->listen_sock) { 
        server->listen_sock = -1;
        close(u64.parts.socket);
        while(start_listen(server)) { usleep(1000); }
    } else if(u64.parts.socket==server->main_thread_pipe[0] ||
              u64.parts.socket==server->main_thread_pipe[1]) {
        server->main_thread_pipe[0] = server->main_thread_pipe[1] = -1;
        while(setup_main_thread_pipe(server)) { usleep(1000); }
    } else {socket_close(server, stack, u64.parts.socket); }
    
    return 1;
}

static inline
int service_thread_quit_alarm(struct server *server, union epoll_u64 u64)
{
    if (u64.parts.socket==server->main_thread_pipe[PIPE_READ]) { 
        epoll_socket_rearm(server, server->main_thread_pipe[PIPE_READ], 
                           0, 0);
        return 1;
    }
    return 0;
}

void *service_thread(void *vserver)  //Data points to the global
{
    if(no_sigs()) { pthread_exit(NULL); }
    
    struct server *server = (struct server *)vserver;
    vector<char> stack;
    int rslt=0;
    
    char first = 1;
    while(1) {
        if(stack.size()!=0) { serv_segfault(FN, "stack corruption"); }
        struct epoll_event evnt;
        if(service_thread_wait(server, &evnt, first)) { continue; } 
        const union epoll_u64 u64 = { val:evnt.data.u64 };
        first=0;
        if(socket_handle_errors(server, &stack, &evnt)) { continue; }
        if(!(evnt.events & EPOLLIN)) { 
            serv_error("%s() no EPOLLIN, must be unexpected event:%d. closing socket\n", FN, evnt.events); 
            socket_close(server, &stack, u64.parts.socket); continue;
        }
        if(service_thread_quit_alarm(server, u64) ) { break; }
        if (u64.parts.socket==server->listen_sock) { 
            create_downstream(server, server->listen_sock); 
            continue;
        }
        int final = buffer_recv(server, u64.parts.socket);
        if(u64.parts.meta.is_upstream) { 
            rslt = process_messages(server, u64.parts.socket);
        } else rslt = process_commands(server, &stack, u64.parts.socket,
                                       u64.parts.meta.is_local);
        if(final || rslt < 0) { socket_close(server, &stack, u64.parts.socket); }
        else { 
            epoll_socket_rearm(server, u64.parts.socket, 
                               u64.parts.meta.is_upstream,
                               u64.parts.meta.is_local);
        }
    }
    futex_down2(&server->service_threads_sem, FN);
    pthread_exit(NULL);
}

int spawn_service_thread(struct server *server)
{
    pthread_t thread;
    if(futex_trydown(&server->service_thread_factory_lock)) { return -1; }
    
    int rslt = 
          pthread_create(&thread, NULL, service_thread, (void *)server);
    if(rslt) {
        serv_error("%s() failed on pthread_create() which returned:%d\n",
                   FN, rslt);
        return -2;
    }
    
    futex_up(&server->service_threads_sem);
    return 0;
}


int server_start(struct server *server,
                 int argc,
                 char *argv[])
{
    server->term_requested = 0;
    server->listen_sock = -1;
    server->main_thread = pthread_self();
    server->local_ip = inet_addr("127.0.0.1");
    server->listen_port = argv[1];
    
    server->epoll_fd = epoll_create1(0); 
    if(server->epoll_fd==-1) { 
        serv_perror("server_start() failed on epoll_create()");
        return -1; 
    }
    if(setup_main_thread_pipe(server)) { return -2; }
    futex_init(&server->recv_queue_lock);
    futex_init(&server->service_thread_factory_lock);
    futex_init(&server->host_socket_lock);
    futex_init(&server->epoll_wait_threads);
    futex_init(&server->service_threads_sem);
    server->epoll_wait_threads.resource =
          server->service_threads_sem.resource = 0;
    
    for(int i=0; i<N_ROT_LOCKS; i++) {
        futex_init(&server->socket_read_buf_lock[i]);
    }
    server->crypto_fd = open("/dev/urandom", O_RDONLY);
    if(server->crypto_fd==-1) { 
        serv_perror(FN);
        serv_segfault("%s() crypto open() failed", FN); 
    }

    if(engine_init(&server->engine) ||
       start_listen(server) ||
       spawn_service_thread(server)) {
        return -3;
    }
    
    return 0;
}

void term_service_threads(struct server *server)
{
    futex_down2(&server->service_thread_factory_lock, FN);
    char cmd = 0;
    //int i = server->service_threads_sem.resource; 
    //while(i) { 
    int rslt = -1;
    while(rslt<=0) {
        rslt = write(server->main_thread_pipe[PIPE_WRITE], &cmd, sizeof(cmd));
        if(rslt) { 
            serv_perror("server_term on writing into main_thread_pipe"); 
            usleep(1000);
        }
    //    else{ i--; }
    }
    //now wait until they are all done
    while(server->service_threads_sem.resource) { usleep(1000); }
}

int server_term(struct server *server)
{
    close(server->listen_sock);  //kernel will remove from epoll.
    server->listen_sock = -1;
    
    vector<char> stack;
    
    lock_recv_queue(server);
    while(server->recv_queue.size()) {
        auto it = server->recv_queue.begin(); 
        if(it==server->recv_queue.end()) { break; }
        unlock_recv_queue(server); //so socket_close doesn't deadlock
        socket_close(server, &stack, it->first);
        lock_recv_queue(server);
    }
    unlock_recv_queue(server);
    
    term_service_threads(server);
    
    close(server->epoll_fd); server->epoll_fd=-1;
    
    return engine_term(&server->engine);
}

struct server server0;

static void
handler(int sig, siginfo_t *si, void *unused)
{
    const char *signal_name = "unexpected";
    if(     sig==SIGINT ) { signal_name = "SIGINT";  }
    else if(sig==SIGTERM) { signal_name = "SIGTERM";  }
    serv_error("%s received. shutting down...", signal_name);
    server0.term_requested = 1;
}

int sigs()
{
    struct sigaction sa;
    
    sa.sa_flags = SA_SIGINFO;
    sigemptyset(&sa.sa_mask);
    sa.sa_sigaction = handler;
    uint32_t rslt0 = (uint32_t) sigaction(SIGINT, &sa, NULL);
    if(rslt0) { serv_perror("sigaction SIGINT"); }
    
    return (int)(rslt0);
}

int main(int argc, char *argv[])
{
    if(argc<2) { serv_error("usage: pubsub-server LISTEN_PORT\n"); return 1; }
    if(sigs()) { serv_error("%s() failed at sigs(), terminating!\n", FN); return 1; }
    if(server_start(&server0, argc, argv)==0 && 
       !server0.term_requested) {
        pause(); //will break on caught signal
    }
    server_term(&server0);
    
    dbg_printf(FN, LN, "clean shutdown.");
    return 0;
}

