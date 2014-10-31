#ifndef pub_sub_H_DEFINED
#define pub_sub_H_DEFINED

#include "common.h" 
#include "usersem.h" 
#include "varmap.h"
#include "cvector.h"
#include "err.h"
#include "misc.h"
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <deque>

using namespace std;

#define N_STREAMS 100
#define N_ROT_LOCKS 1024 //this should be much bigger than # of cores

struct stream_indx_entry {
    uint64_t timestamp;
    uint64_t offst;
}__attribute__((__packed__));

struct msg_header {
    uint64_t len;
    uint64_t timestamp;
    uint32_t stream_id;
    char buf[];
}__attribute__((__packed__));

struct stream {
    struct rwlock subscribers_lock;
    struct rwlock map_lock;
    struct varmap *content;
    struct varmap *indx;
    class unordered_set<int> subscribers;
    uint32_t id;
    int master_socket;
    char is_master;
};

//returns -1 meaning this is not a master or slave stream
//         0 meaning master
//         otherwise the slave socket which is clearly not 0 (stdin)
static inline int stream_master_socket(struct stream *s)
{
    read_lock(&s->map_lock);
    int sock = s->master_socket;
    if(sock == -1 && s->is_master) { sock = 0; }
    read_unlock(&s->map_lock);
    
    return sock;
}

struct engine {
    struct semaphore socket_write_lock[N_ROT_LOCKS];
    struct semaphore socket_write_buf_lock[N_ROT_LOCKS];
    struct stream streams[N_STREAMS];
    struct semaphore subscriptions_lock;
    struct semaphore send_queue_lock;
    unordered_multimap<int, uint32_t> downstreams;
    unordered_map<int, vector<char> * > send_queue;
};

int engine_init(struct engine *engine);
int engine_term(struct engine *engine);

int stream_write(struct engine *engine,
                 void *buf, 
                 const size_t len,
                 const int src_sock);

//send all history from catchup_from before pushing new messages
int stream_subscribe(struct engine *engine,
                     const uint32_t stream_id,
                     const int socket,
                     uint64_t catchup_from);

//stops the stream push to the socket
void stream_unsubscribe(struct engine *engine,
                        const uint32_t stream_id,
                        const int socket);

//unsubscribe socket from all streams
//this is extremely important to call before issuing a close() on
//the socket, otherwise the kernel will reassign the socket for a new
//connection and that connection will start getting pushed data meant
//for the old relic connection that was already closed.
void socket_unsubscribe(struct engine *engine,
                        const int socket);

int noblock_send(struct engine *engine,
                 const int socket, 
                 const void *buf, 
                 const size_t len);

void socket_flush(struct engine *engine, const int socket);

void destroy_socket_send_queue(struct engine *engine,
                               const int socket);

static inline int trylock_socket(struct engine *engine,
                                 const int socket)
{
    return futex_trydown(&engine->socket_write_lock[socket%N_ROT_LOCKS]);
}

static inline void lock_socket(struct engine *engine,
                               const int socket)
{
    while(futex_down(&engine->socket_write_lock[socket%N_ROT_LOCKS])) { 
        serv_perror(FN); 
    }
}

//should really use flush_unlock_socket which uses this one
//   otherwise you may get zombie bytes sitting in send buffers.
static inline void unlock_socket(struct engine *engine,
                                 const int socket)
{
    futex_up(&engine->socket_write_lock[socket%N_ROT_LOCKS]);
}

static inline int trylock_socket_buf(struct engine *engine,
                                     const int socket)
{
    return futex_trydown(&engine->socket_write_buf_lock[socket%N_ROT_LOCKS]);
}

static inline void lock_socket_buf(struct engine *engine,
                                   const int socket)
{
    while(futex_down(&engine->socket_write_buf_lock[socket%N_ROT_LOCKS])) { 
        serv_perror(FN); 
    }
}

static inline void unlock_socket_buf(struct engine *engine,
                                     const int socket)
{
    futex_up(&engine->socket_write_buf_lock[socket%N_ROT_LOCKS]);
}

static inline
void msg_header_init(struct msg_header *hdr,
                     uint32_t stream_id,
                     uint64_t timestamp,
                     uint64_t len)
{
    BZERO(hdr);
    hdr->stream_id = stream_id;
    hdr->timestamp = timestamp;
    hdr->len = len;
}

//Clients publishing messages (written in php or whatever...) should
//   implement a function that does this and writes the result to the
//   socket they have with the publishing node. It's basically adding a 
//   header to the message that informs the recipient of stream_id
//   len, and timestamp. If the clients are running in browsers, it
//   might be easier to encode those fields separately using JSON,
//   but for this exercise, I am using binary, optimizing the backend,
//   and not worrying about frontend being able to produce the C struct
//   binary layout.
          //timestamped by master engine for this stream upon receipt
          //the recipient will continue to pass it to whom he thinks
          //the master is, until it gets there. The master will timestamp
          //and commit(meaning send to all subscribers).
          //to all subscribers. Each subscribing engine will then write
          //it locally, 
static inline
void message_create(class vector<char> *stack,
                    const uint32_t stream_id,
                    const void *buf, 
                    const size_t len)
{
    struct msg_header header;
    msg_header_init(&header, stream_id, 0, len);
    
    CVECTOR_APPEND(stack, &header);
    cvector_append(stack, buf, len);
    cvector_nt(stack);
}

#endif //pub_sub_H_DEFINED
