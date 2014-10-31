#include "err.h"

struct semaphore stderr_lock;
struct semaphore stdout_lock;

__attribute__((constructor)) 
void global_locks_init()
{
    futex_init(&stderr_lock);
    futex_init(&stdout_lock);
}
