#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/event.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/queue.h>

#define PORT 8080
#define BACKLOG 10
#define BUFFER_SIZE 1024
#define MAX_EVENTS 10
#define CLIENT_TIMEOUT 5000  // 5 seconds in milliseconds


// Task struct for the queue 
typedef struct task_t {
    void (*function)(void*);  // Function to be executed
    void* argument;           // Argument to the function
    TAILQ_ENTRY(task_t) entries;  // Tail queue macro entries
} task_t;

// Thread-safe queue using BSD TAILQ macros
typedef struct {
    TAILQ_HEAD(task_queue, task_t) queue_head;  // Head of the queue
    pthread_mutex_t lock;   // Mutex for thread safety
    pthread_cond_t cond;    // Condition variable for signaling
    int stop;               // Flag to stop the queue processing
} task_queue_t;

// Initialize the task queue
void task_queue_init(task_queue_t* queue) {
    TAILQ_INIT(&queue->queue_head);  // Initialize the queue
    pthread_mutex_init(&queue->lock, NULL);
    pthread_cond_init(&queue->cond, NULL);
    queue->stop = 0;
}

// Add a task to the queue
void task_queue_push(task_queue_t* queue, void (*function)(void*), void* argument) {
    task_t* new_task = (task_t*)malloc(sizeof(task_t));
    new_task->function = function;
    new_task->argument = argument;

    pthread_mutex_lock(&queue->lock);
    TAILQ_INSERT_TAIL(&queue->queue_head, new_task, entries);  // Insert at the tail of the queue
    pthread_cond_signal(&queue->cond); // Signal waiting threads that a new task is available
    pthread_mutex_unlock(&queue->lock);
}

// Pop a task from the queue
task_t* task_queue_pop(task_queue_t* queue) {
    pthread_mutex_lock(&queue->lock);
    while (TAILQ_EMPTY(&queue->queue_head) && !queue->stop) {
        pthread_cond_wait(&queue->cond, &queue->lock);  // Wait for tasks to be added
    }

    if (queue->stop) {
        pthread_mutex_unlock(&queue->lock);
        return NULL;  // Stop signal received
    }

    task_t* task = TAILQ_FIRST(&queue->queue_head);  // Get the first task in the queue
    TAILQ_REMOVE(&queue->queue_head, task, entries);  // Remove from the queue

    pthread_mutex_unlock(&queue->lock);
    return task;
}

// Destroy the task queue
void task_queue_destroy(task_queue_t* queue) {
    pthread_mutex_lock(&queue->lock);
    queue->stop = 1;
    pthread_cond_broadcast(&queue->cond);  // Wake up all waiting threads
    pthread_mutex_unlock(&queue->lock);
}


// Thread pool struct
typedef struct {
    task_queue_t queue;
    int num_threads;
    pthread_t* threads;
} thread_pool_t;

// Worker function
void* thread_worker(void* arg) {
    thread_pool_t* pool = (thread_pool_t*)arg;

    while (1) {
        task_t* task = task_queue_pop(&pool->queue);
        if (task == NULL) {
            break;  // Stop signal received
        }

        task->function(task->argument);  // Execute the task
        free(task);  // Free the task memory
    }

    return NULL;
}

// Initialize thread pool
void thread_pool_init(thread_pool_t* pool, int num_threads) {
    pool->num_threads = num_threads;
    pool->threads = (pthread_t*)malloc(num_threads * sizeof(pthread_t));
    task_queue_init(&pool->queue);

    for (int i = 0; i < num_threads; ++i) {
        pthread_create(&pool->threads[i], NULL, thread_worker, pool);
    }
}

// Add a task to the pool
void thread_pool_add_task(thread_pool_t* pool, void (*function)(void*), void* argument) {
    task_queue_push(&pool->queue, function, argument);
}

// Destroy thread pool
void thread_pool_destroy(thread_pool_t* pool) {
    task_queue_destroy(&pool->queue);

    for (int i = 0; i < pool->num_threads; ++i) {
        pthread_join(pool->threads[i], NULL);
    }

    free(pool->threads);
}


// Non-blocking function for sockets
int set_non_blocking(int sockfd) {
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl");
        return -1;
    }
    return fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
}

// Function to remove client from kqueue
void remove_client_event(int kq, int client_fd) {
    struct kevent ev_set;

    // Remove read event from kqueue
    EV_SET(&ev_set, client_fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    kevent(kq, &ev_set, 1, NULL, 0, NULL);

    // Remove timer event from kqueue
    EV_SET(&ev_set, client_fd, EVFILT_TIMER, EV_DELETE, 0, 0, NULL);
    kevent(kq, &ev_set, 1, NULL, 0, NULL);
}

// Function to handle client timeouts
void handle_timeout(int kq, int client_fd) {
    printf("Client connection %d timed out\n", client_fd);
    remove_client_event(kq, client_fd);
    close(client_fd);  // Close the client connection
}

// Dummy database query (blocking operation)
char* perform_database_query() {
    sleep(2);  // Simulate a delay
    return strdup("Database query result\n");
}

// Function to handle the database task
void db_query_task(void* arg) {
    int client_fd = *(int*)arg;

    // Perform a database operation (this is blocking)
    char* db_result = perform_database_query();

    // handle the db result here
    // write(client_fd, db_result, strlen(db_result));

    free(db_result);
    free(arg);  // Free the argument (client_fd pointer)
}

// Function to handle client requests
void handle_client(int kq, int client_fd, thread_pool_t* db_thread_pool) {
    char buffer[BUFFER_SIZE];
    ssize_t bytes_read;
    int done = 0;

    while(1) {
        int bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);  
        if (bytes_read > 0) {

            buffer[bytes_read] = '\0';
            printf("Received from client: %s\n", buffer);

            // Offload the database query to the thread pool
            int* client_fd_ptr = (int*)malloc(sizeof(int));
            *client_fd_ptr = client_fd;
            thread_pool_add_task(db_thread_pool, db_query_task, client_fd_ptr);

             const char *response =
                "HTTP/1.1 200 OK\n"
                "Content-Type: text/plain\n"
                "Connection: close\n"
                "\n"
                "Hello, World!\n";

            write(client_fd, response, strlen(response));


        } else if (bytes_read == 0) {
            printf("Client disconnected\n");
            done = 1;
            break;
        } else {
         // Handle errors
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No data available, but the connection is still open; wait for more data
                printf("No data available, retrying later\n");
                break;  // Exit the read loop and return to event loop
            } else if (errno == EINTR) {
                // The read call was interrupted by a signal, retry reading
                printf("Read interrupted by signal, retrying\n");
                continue;
            } else {
                perror("read");
                done = 1;
                break;
            }
        }
    }

     // If done, close the connection and clean up
    if (done) {
        remove_client_event(kq, client_fd);
        close(client_fd);
    }
}

// Main server code using kqueue and thread pool
int main() {
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t addr_len;
    int kq, nev, i;
    struct kevent ev_set, ev_list[MAX_EVENTS];

    // Create thread pool for database operations
    thread_pool_t db_thread_pool;
    thread_pool_init(&db_thread_pool, 4);  // 4 worker threads

    // Create server socket
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, BACKLOG) < 0) {
        perror("listen");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    set_non_blocking(server_fd);

    kq = kqueue();
    EV_SET(&ev_set, server_fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, NULL);
    kevent(kq, &ev_set, 1, NULL, 0, NULL);

    printf("Server listening on port %d...\n", PORT);

    while (1) {
        nev = kevent(kq, NULL, 0, ev_list, MAX_EVENTS, NULL);
        for (i = 0; i < nev; i++) {
            if (ev_list[i].ident == server_fd) {
                addr_len = sizeof(client_addr);
                client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
                set_non_blocking(client_fd);

                EV_SET(&ev_set, client_fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, NULL);
                kevent(kq, &ev_set, 1, NULL, 0, NULL);

                 // Add a timeout event for the client
                EV_SET(&ev_set, client_fd, EVFILT_TIMER, EV_ADD | EV_ENABLE, 0, CLIENT_TIMEOUT, NULL);
                kevent(kq, &ev_set, 1, NULL, 0, NULL);

                printf("Accepted new connection: %d\n", client_fd);
            } else if (ev_list[i].filter == EVFILT_READ) {
                handle_client(kq, ev_list[i].ident, &db_thread_pool);
            } else if (ev_list[i].filter == EVFILT_TIMER) {
                // Handle client timeout
                handle_timeout(kq, ev_list[i].ident);
            }
        }
    }

    close(server_fd);
    thread_pool_destroy(&db_thread_pool);
    return 0;
}
