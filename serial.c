/********************************************************************
* Author -      Vraj Patel, Deepey Panchal, Hieu Nguyen             *
* NetIDs -      patelv27, deepeypradippanchal,                      *
* Program -     Video Compression Tool                              *
* Description - This program implements a compression tool that     *
*               takes multiple files representing a video's frames  *
*               and compresses them into a single output zip. The   *
*               program effectively utilizes up to 20 threads to    *
*               speed up the otherwise timely compression process.  *
*********************************************************************/
#include <dirent.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <time.h>
#include <pthread.h>

#define BUFFER_SIZE 1048576 // 1MB
#define MAX_THREADS 19      // Max number of allowed threads

// represents the compressed file data - i.e. output of compressing 1 file
typedef struct {
    int input_size;            // Size of the input file
    int size;                  // Size of the compressed data
    unsigned char *data;       // Compressed data
} compressed_file_t;

// facilitates passing 'work items' to the worker threads.
typedef struct {
    int index;                     // Index of the file in the files array
    char *directory;               // Directory containing the files
    char *filename;                // Filename to process
} work_item_t;

// this is a queue for nodes implemented as a linked list
typedef struct work_node {
    work_item_t *work_item;
    struct work_node *next;
} work_node_t;

// structure that stores all threads
typedef struct {
    pthread_t threads[MAX_THREADS];
    work_node_t *work_queue_head;    // points to start of linked list
    work_node_t *work_queue_tail;    // points to end of linked list
    pthread_mutex_t queue_mutex;    // stores locks
    pthread_cond_t queue_cond;    //
    int stop;
} thread_pool_t;

// Global variables
compressed_file_t *compressed_files;
int total_in = 0, total_out = 0;
char **files = NULL;
int nfiles = 0;
char *directory;

// these are function prototypes defined below
void *worker_thread(void *arg);
void enqueue_work(thread_pool_t *pool, work_item_t *work_item);
work_item_t *dequeue_work(thread_pool_t *pool);
void thread_pool_init(thread_pool_t *pool);
void thread_pool_delete(thread_pool_t *pool);
int cmp(const void *a, const void *b);

// main function is the entry point
int main(int argc, char **argv) {
    // Time computation header
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    // End of time computation header

    // Do not modify the main function before this point!

    assert(argc == 2);

    DIR *d;
    struct dirent *dir;
    nfiles = 0;
    directory = argv[1];

    d = opendir(directory);
    if (d == NULL) {
        printf("An error has occurred\n");
        return 0;
    }

    // create a sorted list of input PPM files
    while ((dir = readdir(d)) != NULL) {
        int len = strlen(dir->d_name);
        if (len >= 4 && strcmp(dir->d_name + len - 4, ".ppm") == 0) {
            files = realloc(files, (nfiles + 1) * sizeof(char *));
            assert(files != NULL);
            files[nfiles] = strdup(dir->d_name);
            assert(files[nfiles] != NULL);
            nfiles++;
        }
    }
    closedir(d);
    qsort(files, nfiles, sizeof(char *), cmp);

    // Initialize array to store compressed file data
    compressed_files = malloc(nfiles * sizeof(compressed_file_t));
    assert(compressed_files != NULL);
	  int i;
    for (i = 0; i < nfiles; i++) {
        compressed_files[i].input_size = 0;
        compressed_files[i].size = 0;
        compressed_files[i].data = NULL;
    }

    // Initialize thread pool
    thread_pool_t pool;
    thread_pool_init(&pool);

    // enqueue work items
    for (i = 0; i < nfiles; i++) {
        work_item_t *work_item = malloc(sizeof(work_item_t));
        assert(work_item != NULL);
        work_item->index = i;
        work_item->directory = directory;
        work_item->filename = files[i];
        enqueue_work(&pool, work_item);
    }

    // Destroy thread pool after all work is done
    thread_pool_delete(&pool);

    // Create a single zipped package with all PPM files in lexicographical order
    FILE *f_out = fopen("video.vzip", "wb");
    assert(f_out != NULL);

    for (i = 0; i < nfiles; i++) {
        fwrite(&compressed_files[i].size, sizeof(int), 1, f_out);
        fwrite(compressed_files[i].data, sizeof(unsigned char), compressed_files[i].size, f_out);
        total_in += compressed_files[i].input_size;
        total_out += compressed_files[i].size;
        free(compressed_files[i].data); // Free compressed data
    }
    fclose(f_out);

    printf("Compression rate: %.2lf%%\n", 100.0 * (total_in - total_out) / total_in);

    // Release the compressed files array
    free(compressed_files);

    // Release list of files
    for (i = 0; i < nfiles; i++)
        free(files[i]);
    free(files);

    // Do not modify the main function after this point!

    // Time computation footer
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Time: %.2f seconds\n", ((double)end.tv_sec + 1.0e-9 * end.tv_nsec) - ((double)start.tv_sec + 1.0e-9 * start.tv_nsec));
    // End of time computation footer

    return 0;
}

/**
 * Comparator function supplied to qsort to sort filenames lexicographically.
 */
int cmp(const void *a, const void *b) {
    return strcmp(*(char **) a, *(char **) b);
}

/**
 * Worker thread function to process files.
 */
void *worker_thread(void *arg) {
    thread_pool_t *pool = (thread_pool_t *) arg;
    while (1) {
        pthread_mutex_lock(&pool->queue_mutex);
        // wait while the work queue is empty and the stop signal is off
        while (pool->work_queue_head == NULL && !pool->stop) {
            pthread_cond_wait(&pool->queue_cond, &pool->queue_mutex);
        }
        // if stop signal is on but work queue is still empty, break and unlock mutex
        if (pool->stop && pool->work_queue_head == NULL) {
            pthread_mutex_unlock(&pool->queue_mutex);
            break;
        }
        // otherwise, continue with processing and obtain next work item
        work_item_t *work_item = dequeue_work(pool);
        pthread_mutex_unlock(&pool->queue_mutex); // release lock - not needed until we pull next item

        if (work_item) {
            int index = work_item->index;
            char *filename = work_item->filename;

            // Build the full path to the file as in the provided code
            int len = strlen(directory) + strlen(filename) + 2;
            char *full_path = malloc(len * sizeof(char));
            assert(full_path != NULL);
            strcpy(full_path, directory);
            strcat(full_path, "/");
            strcat(full_path, filename);

            // Allocate buffers on the heap (shared spaced amongst threads)
            unsigned char *buffer_in = malloc(BUFFER_SIZE * sizeof(unsigned char));
            assert(buffer_in != NULL);
            unsigned char *buffer_out = malloc(BUFFER_SIZE * sizeof(unsigned char));
            assert(buffer_out != NULL);

            // Load the input file
            FILE *f_in = fopen(full_path, "rb"); // open file in binary mode
            assert(f_in != NULL);
            int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
            fclose(f_in);

            // Store the size of the input file
            compressed_files[index].input_size = nbytes;

            // Compress the data using zlib
            z_stream strm;
            int ret = deflateInit(&strm, 9);
            assert(ret == Z_OK);
            strm.avail_in = nbytes;
            strm.next_in = buffer_in;
            strm.avail_out = BUFFER_SIZE;
            strm.next_out = buffer_out;

            ret = deflate(&strm, Z_FINISH);
            assert(ret == Z_STREAM_END);
            deflateEnd(&strm);

            // Store the compressed data into the globally defined array
            int nbytes_zipped = BUFFER_SIZE - strm.avail_out;
            compressed_files[index].data = malloc(nbytes_zipped * sizeof(unsigned char));
            assert(compressed_files[index].data != NULL);
            memcpy(compressed_files[index].data, buffer_out, nbytes_zipped);
            compressed_files[index].size = nbytes_zipped;

            // free all dynamically allocated pointers
            free(buffer_in);
            free(buffer_out);
            free(full_path);
            free(work_item); // free the completed work item
        }
    }
    return NULL;
}

/**
 * Enqueue a work item to the thread pool's work queue.
 */
void enqueue_work(thread_pool_t *pool, work_item_t *work_item) {
    work_node_t *node = malloc(sizeof(work_node_t));
    assert(node != NULL);
    node->work_item = work_item;
    node->next = NULL;

    // add new node to the end of the linked list
    // concurrency lock needed here to prevent orphan nodes
    pthread_mutex_lock(&pool->queue_mutex);
    if (pool->work_queue_tail == NULL) {  // if empty queue
        pool->work_queue_head = node;
        pool->work_queue_tail = node;
    } else {
        pool->work_queue_tail->next = node;
        pool->work_queue_tail = node;
    }
    pthread_cond_signal(&pool->queue_cond);
    pthread_mutex_unlock(&pool->queue_mutex);
}

/**
 * Dequeue a work item from the thread pool's work queue.
 */
work_item_t *dequeue_work(thread_pool_t *pool) {
    work_node_t *node = pool->work_queue_head;
    if (node == NULL) {
        return NULL;
    }
    pool->work_queue_head = node->next;
    if (pool->work_queue_head == NULL) {  // if resulting list is empty
        pool->work_queue_tail = NULL;
    }
    work_item_t *work_item = node->work_item;
    free(node);
    return work_item;
}

/**
 * Initialize the thread pool.
 */
void thread_pool_init(thread_pool_t *pool) {
    pool->work_queue_head = NULL;
    pool->work_queue_tail = NULL;
    pool->stop = 0;
    pthread_mutex_init(&pool->queue_mutex, NULL);
    pthread_cond_init(&pool->queue_cond, NULL);
	  int i;
    for (i = 0; i < MAX_THREADS; i++) {
        pthread_create(&pool->threads[i], NULL, worker_thread, pool);
    }
}

/**
 * Destroy the thread pool.
 */
void thread_pool_delete(thread_pool_t *pool) {
    pthread_mutex_lock(&pool->queue_mutex);
    pool->stop = 1;
    pthread_cond_broadcast(&pool->queue_cond);
    pthread_mutex_unlock(&pool->queue_mutex);
	int i;
    for (i = 0; i < MAX_THREADS; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    pthread_mutex_destroy(&pool->queue_mutex);
    pthread_cond_destroy(&pool->queue_cond);
}
