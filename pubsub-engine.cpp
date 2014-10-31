#include "pubsub-engine.h"
#include <sys/types.h>
#include <sys/socket.h>
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
#include <unordered_map>

////////////////////////////////////////////
using namespace std;

static inline 
void stream_indx_entry_init(struct stream_indx_entry *entry,
                            uint64_t offst,
                            uint64_t timestamp)
{
    BZERO(entry);
    
    entry->offst = offst;
    entry->timestamp = timestamp;
}

static inline int trylock_send_queue(struct engine *engine)
{
    return futex_trydown(&engine->send_queue_lock);
}

static inline void lock_send_queue(struct engine *engine)
{
    while(futex_down(&engine->send_queue_lock)) { 
        serv_perror(FN); 
    }
}

static inline void unlock_send_queue(struct engine *engine)
{
    futex_up(&engine->send_queue_lock);
}

static inline int send_bytes_opt(const int socket, 
                                 const void *buf, 
                                 const size_t len,
                                 const int opt)
{
    if(len==0) { return 0; }
    
    const char *cbuf = (const char *)buf;
    
    size_t remaining = len, sent=0;
    while(remaining) {
        ssize_t rslt = send(socket, cbuf+sent, remaining, opt);
        if(rslt < 0) { serv_perror(FN); }
        if(rslt <=0) { 
            //we can't wait on this guy. buffer full or conn err. kill him.
            //SHUT_RDWR makes it pop out of epoll with HUP and
            // ensures following writes don't succeed (respecting sequence).
            shutdown(socket, SHUT_RDWR); 
            return -1; 
        }
        sent+=rslt;
        remaining-=rslt;
    }
    
    return 0;
}

static inline int send_bytes(const int socket, 
                             const void *buf, 
                             const size_t len)
{
    return send_bytes_opt(socket, buf, len, 0);
}

static inline int send_bytes_cork(const int socket, 
                                  const void *buf, 
                                  const size_t len)
{
    return send_bytes_opt(socket, buf, len, MSG_MORE);
}

//a flush of the send buffer, ensuring bytes that could not have been
// written while this thread had lock_socket() do get written now. If there
// is such data, during the flush, allow new send buffer for the socket 
// to be created in send_queue and loop to get it, until all has been sent. 
//the function assumes that the caller has the lock_socket() for the duration
void socket_flush(struct engine *engine, const int socket)
{
    lock_send_queue(engine);
    
    while(1) {
        vector<char> *v = NULL;
        auto it = engine->send_queue.find(socket);
        if(it == engine->send_queue.end()) { break; } //until nothing left
        v = it->second; 
        engine->send_queue.erase(it);
        unlock_send_queue(engine);
        unlock_socket_buf(engine, socket); //so other's can buffer
        if(v) { 
            send_bytes(socket, &(*v)[0], v->size()); 
            delete v; v=NULL;
        }
        lock_socket_buf(engine, socket);
        lock_send_queue(engine);
    } 
    
    unlock_send_queue(engine);    
}

void destroy_socket_send_queue(struct engine *engine,
                               const int socket)
{
    lock_send_queue(engine);
    
    auto it = engine->send_queue.find(socket);
    if(it != engine->send_queue.end()) {
    
        vector<char> *v = it->second; 
        engine->send_queue.erase(it);
        if(v) { delete v; v=NULL; }
    }
    
    unlock_send_queue(engine);
}

static inline 
int noblock_send_core(struct engine *engine,
                      const int socket, 
                      const void *buf, 
                      const size_t len)
{
    if(trylock_socket(engine, socket)==0) {
        unlock_socket_buf(engine, socket);
        int rslt = send_bytes(socket, buf, len); 
        lock_socket_buf(engine, socket);
        socket_flush(engine, socket);
        unlock_socket(engine, socket);
        return rslt;
    }
        
    vector<char> *v = NULL;
    lock_send_queue(engine);
    auto it = engine->send_queue.find(socket);
    if(it!=engine->send_queue.end()) { v = it->second; }
    unlock_send_queue(engine);
    int existed = v?1:0;
    if(!v) { v = new vector<char>; }
    
    cvector_append(v, buf, len); 
    if(!existed) {
        lock_send_queue(engine);
        engine->send_queue[socket]= v;
        unlock_send_queue(engine);
    }
    return 0;
}    

int noblock_send(struct engine *engine,
                 const int socket, 
                 const void *buf, 
                 const size_t len)
{
    lock_socket_buf(engine, socket);
    
    int rslt = noblock_send_core(engine, socket, buf, len);
    
    unlock_socket_buf(engine, socket);
    
    return rslt;
}

static inline ssize_t file_send_core(const int socket, 
                                     const int fd,
                                     const off_t offst,
                                     const size_t len)
{
    int rslt=0;
    off_t scan = offst;
    off_t ol = offst+len;
    if(ol<offst) { return -1; }
    
    while(scan<ol) {
        if(rslt<0) { serv_error("calling sendfile(ywfilesend() failed on sendfile"); } 
        rslt = sendfile(socket,fd, &scan, ol-scan);
        if(rslt<0) { serv_perror("ywfilesend() failed on sendfile"); } 
        if(rslt<=0) { 
            shutdown(socket, SHUT_RDWR); 
            return -1;
        }
    } 
    
    return 0;
}

static inline 
ssize_t file_send(struct engine *engine,
                  const int socket, 
                  const int fd,
                  const off_t offst,
                  const size_t len)
{
    lock_socket(engine, socket);
    set_socket_block(socket, 20*1000*1000); //block for 20 sec, Think about DOS!!!
    
    const size_t rslt = file_send_core(socket, fd, offst, len);
    
    lock_socket_buf(engine, socket);
    socket_flush(engine, socket);
    set_socket_non_block(socket);
    unlock_socket(engine, socket);
    //now noblock_send() would get the socket lock itself
    unlock_socket_buf(engine, socket);
    
    return rslt;
}

int stream_map(struct stream *s)
{
#define FNAME_MAX 32
    char filename[FNAME_MAX];
    
    snprintf(filename, FNAME_MAX-1, "%08x.content", s->id);
    s->content = varmap_init(filename);
    if(!s->content) { return -1; }
    
    snprintf(filename, FNAME_MAX-1, "%08x.indx", s->id);
    s->indx = varmap_init(filename);
    if(!s->indx) { return -2; }

#undef FNAME_MAX
    
    return 0;
}

int stream_unmap(struct stream *s)
{
    if(varmap_close(&s->content) || 
       varmap_close(&s->indx)) { return -1; }
    
    return 0;
}

int stream_open(struct stream *s,
                const uint32_t stream_id)
{
    s->id = stream_id;
    s->master_socket = -1;
    s->is_master = 0;
    
    if(stream_map(s)) { return -1; }
    
    readwrite_init(&s->map_lock);
    readwrite_init(&s->subscribers_lock);
    return 0;
}

int stream_close(struct stream *s)
{
    write_lock(&s->map_lock);
    write_lock(&s->subscribers_lock);
    
    if(stream_unmap(s)) { return -1; }
    
    return 0;
}

//low level send, doesn't manage the lock. get the subscribers' read_lock
static inline void stream_send(struct engine *engine,
                               struct stream *s,
                               const void *buf,
                               const size_t len)
{
    for(auto it=s->subscribers.begin(); 
        it != s->subscribers.end(); it++) {
        
        noblock_send(engine, *it, buf, len);
    }
}

//high level, manages locks, calls stream_send implicitly
int stream_write(struct engine *engine,
                 void *buf, 
                 const size_t len,
                 const int src_sock)
{
    struct msg_header *hdr = (struct msg_header *)buf;
    if(hdr->timestamp == 0 ) { hdr->timestamp = gtime_usec(); }
    struct stream *s = &engine->streams[hdr->stream_id];
    
    write_lock(&s->map_lock);
    
    const int is_allowed =
          ((src_sock != -1 && s->master_socket == src_sock) ||
           (src_sock == -1 && s->is_master ) );
    
    if(!is_allowed) { write_unlock(&s->map_lock); return 1; }
    
    dbg_printf(FN, LN, "msg:%s", hdr->buf);
    
    struct stream_indx_entry indx;
    stream_indx_entry_init(&indx, s->content->len, hdr->timestamp);
    varmap_append(&(s->content), buf, len, MS_ASYNC);
    varmap_APPEND(&(s->indx), &indx, MS_ASYNC); 
                  
    read_lock(&s->subscribers_lock);
    write_unlock(&s->map_lock);
    
    stream_send(engine, s, buf, len);
    
    read_unlock(&s->subscribers_lock);
    return 0;
}

//order stream by time, then offset
static inline int stream_time_compare(const void * av,
                                      const void * bv)
{
    const struct stream_indx_entry *a = 
          (const struct stream_indx_entry *) av;
    const struct stream_indx_entry *b =  
          (const struct stream_indx_entry *) bv;
    
    if(a->timestamp < b->timestamp) { return -1; }
    if(a->timestamp > b->timestamp) { return  1; }
    
    if(a->offst < b->offst) { return -1; }
    if(a->offst > b->offst) { return  1; }
    
    return 0;
}

//if this doesn't find anything, it will return where the item would 
//  have been - basically rounding up to the next item or End().
static inline size_t stream_search(struct stream *s,
                                   const uint64_t timestamp)
{
    //find the first item matching that time
    struct stream_indx_entry key;
    const uint64_t offst = 0;
    stream_indx_entry_init(&key, offst, timestamp);
    size_t low = 0;
    size_t off_limits = 
          s->indx->len / sizeof(struct stream_indx_entry);
    while(low<off_limits) {
        int mid = ((off_limits - low) / 2) + low;
        struct stream_indx_entry *test = 
              &VARMAP_ARR(s->indx, typeof(*test))[mid];
        int o = stream_time_compare(&key,  test);
        if(o==0) { return mid; }
        if     (o<0) { off_limits = mid; }
        else if(o>0) {  low = mid+1; }
    }
    
    return off_limits;
}

//NOTE 1:
//  we don't have to maintain the lock while we insert the subscription
//  into the master subscriptions table because another request
//  from the socket is not read (not back in epoll) 
//  until this one has finished. Note that the socket is fully duplex
//  meaning that other streams trying to push to it will not be blocked
//  by this, only other requests from the socket won't be read until it
//  is back in epoll.
//
//NOTE 2: we are sending catchup data with no locks being held
//  we are assuming the content.fd file descriptor does not
//  change. This assumption allows us to unlock the varmap
//  so the stream can append and get remap() while the underlying fd
//  is used for kernel space sendfile() in lower offsets
int stream_subscribe(struct engine *engine,
                     const uint32_t stream_id,
                     const int socket,
                     uint64_t catchup_from)
{
    struct stream *s = &engine->streams[stream_id];
    
    read_lock(&s->map_lock);
    
    const int fd = s->content->fd;  
    size_t from = stream_search(s, catchup_from); //round up
    while(1) {
        size_t to = s->indx->len / sizeof(struct stream_indx_entry);
        if(from >= to) { break; }
        struct stream_indx_entry *arr = 
              VARMAP_ARR(s->indx, struct stream_indx_entry);
        const off_t file_offst = arr[from].offst + sizeof(struct varmap);
        const off_t len = (s->content->len - arr[from].offst);
        
        read_unlock(&s->map_lock); //don't block the stream!
        file_send(engine, socket, fd, file_offst, len);
        read_lock(&s->map_lock);
        from = to; //now loop around to see if the stream advanced beyond "to"
    }
    //caught up on all received content. now add subscriber before
    //   adding any more content
    write_lock(&s->subscribers_lock);
    read_unlock(&s->map_lock);
    s->subscribers.insert(socket);
    write_unlock(&s->subscribers_lock);
    //see NOTE 1 above
    while(futex_down(&engine->subscriptions_lock)) { serv_perror(FN); }
    const pair<int, uint32_t> p(socket, stream_id);
    engine->downstreams.insert(p);
    futex_up(&engine->subscriptions_lock);
    return 0;
}

void stream_unsubscribe2(struct engine *engine,
                         const uint32_t stream_id,
                         const int socket)
{
    struct stream *s = &engine->streams[stream_id];
    
    write_lock(&s->subscribers_lock);
    
    s->subscribers.erase(socket);
    
    write_unlock(&s->subscribers_lock);
}

//does stream_unsubscribe2 and then takes the subscription out of engine too
void stream_unsubscribe(struct engine *engine,
                        const uint32_t stream_id,
                        const int socket)
{
    stream_unsubscribe2(engine, stream_id, socket);
    
    while(futex_down(&engine->subscriptions_lock)) { serv_perror(FN); }
    
    auto range = engine->downstreams.equal_range(socket);
    for(auto it = range.first; it != range.second; it++) {
        if(it->second == stream_id) { 
            engine->downstreams.erase(it); 
            break;
        }
    }
    
    futex_up(&engine->subscriptions_lock);
}

//assumes already locked engine->subscriptions_lock
void socket_unsubscribe2(struct engine *engine,
                         const int socket)
{
    auto range = engine->downstreams.equal_range(socket);
    for(auto it = range.first; it != range.second; it++) {
        stream_unsubscribe2(engine, it->second, it->first);
    }
    engine->downstreams.erase(socket);
}

//unsubscribe socket from all streams
//this is extremely important to call before issuing a close() on
//the socket, otherwise the kernel will reassign the socket for a new
//connection and that connection will start getting pushed data meant
//for the old relic connection that was already closed.
void socket_unsubscribe(struct engine *engine,
                        const int socket)
{
    while(futex_down(&engine->subscriptions_lock)) { serv_perror(FN); }
    
    socket_unsubscribe2(engine, socket);
    
    futex_up(&engine->subscriptions_lock);
}

//unsubscribe socket from all streams
//this is extremely important to call before issuing a close() on
//the socket, otherwise the kernel will reassign the socket for a new
//connection and that connection will start getting pushed data meant
//for the old relic connection that was already closed.
int engine_init(struct engine *engine)
{
    for(int i=0; i<N_ROT_LOCKS; i++) {
        futex_init(&engine->socket_write_lock[i]);
        futex_init(&engine->socket_write_buf_lock[i]);
    }
    
    for(int i=0; i<N_STREAMS; i++) {
        stream_open(&engine->streams[i], i);
    }
    
    futex_init(&engine->subscriptions_lock);
    futex_init(&engine->send_queue_lock);
    
    return 0;
}

int engine_term(struct engine *engine)
{
    for(int i=0; i<N_STREAMS; i++) {
        stream_close(&engine->streams[i]);
    }
    
    return 0;
}
