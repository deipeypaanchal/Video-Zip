/********************************************************************
* Author -      Vraj Patel, Deipy Panchal, Hieu Nguyen              *
* NetIDs -      patelv27, deepeypradippanchal, hieuminhnguyen       *
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

//////////////////////
// Data Definitions //
//////////////////////

#define BUFFER_SIZE 1048576 // 1MB
#define MAX_THREADS 20      // Max number of allowed threads

//////////////////////
// Type Definitions //
//////////////////////

// represents the compressed file data - i.e. output of compressing 1 file
typedef struct {
    int input_size;            // Size of the input file
    int size;                  // Size of the compressed data
    unsigned char *data;       // Compressed data
} compressed_file_t;

// facilitates passing 'work files' to the worker threads.
typedef struct {
    int index;                     // Index of the file in the files array
    char *directory;               // Directory containing the files
    char *filename;                // Filename to process
} work_file_t;

// this is a queue for work files implemented as a linked list
typedef struct work_node {
    work_file_t *work_file;
    struct work_node *next;
} work_node_t;

// structure that stores all threads
typedef struct {
    pthread_t threads[MAX_THREADS];
    work_node_t *work_queue_head;    // points to start of linked list
    work_node_t *work_queue_tail;    // points to end of linked list
    pthread_mutex_t queue_mutex;    // concurrency lock shared across thread pool
    pthread_cond_t queue_cond;    // condition variable shared across thread pool
    int stop;    // when 0, there is work to be done
} thread_pool_t;

/////////////////////////
//  Global Variables   //
/////////////////////////

compressed_file_t *compressed_files;  // array to store all compressed files
int total_in = 0, total_out = 0;  // byte counter
char **files = NULL;   // an array of file names
int nfiles = 0;  // number of files in input directory
char *directory;  // directory stream

/////////////////////////
// Function Prototypes //
/////////////////////////

// the routine that all threads work out of
void *worker_thread(void *arg);

// adds a work file entry to the end of the queue
void enqueue_work(thread_pool_t *pool, work_file_t *work_file);

// pops out a work file entry from the front of the queue
work_file_t *dequeue_work(thread_pool_t *pool);

// initializes the thread pool with the max number of threads
void thread_pool_init(thread_pool_t *pool);

// cleans up by destroying the thread pool
void thread_pool_delete(thread_pool_t *pool);

// extracts the file number from its name to be used as an index
int extract_index(const char *filename);

/******************************************************************
 * main: Reads the input directory of video frames, starts the    *
 *       thread pool, and writes out resulting compression to the *
 *       .vzip file. Also, the main function is responsible for   *
 *       determining the running time and compression size.       * 
 ******************************************************************/
int main(int argc, char **argv) {
    // Time computation header
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    // End of time computation header

    // Do not modify the main function before this point!

    assert(argc == 2);  // must be exactly one command line argument

    // open a directory stream, and read all files within it to an array of strings
    DIR *d;
    struct dirent *dir;
    nfiles = 0;
    directory = argv[1];

    d = opendir(directory);
    if (d == NULL) {
        printf("An error has occurred\n");
        return 0;
    }

    // walk the input directory
    while ((dir = readdir(d)) != NULL) {
        int len = strlen(dir->d_name);
        // if the file extension is ".ppm", read its contents and increment file counter
        if (len >= 4 && strcmp(dir->d_name + len - 4, ".ppm") == 0) {
            files = realloc(files, (nfiles + 1) * sizeof(char *));
            assert(files != NULL);
            files[nfiles] = strdup(dir->d_name);
            assert(files[nfiles] != NULL);
            nfiles++;
        }
    }
    closedir(d);

    // allocate the array that stores compressed file data
    compressed_files = malloc(nfiles * sizeof(compressed_file_t));
    assert(compressed_files != NULL);

    // initialize the thread pool and start the threads
    thread_pool_t pool;
    thread_pool_init(&pool);

    // enqueue each work file to the shared queue
    int i;
    for (i = 0; i < nfiles; i++) {
        work_file_t *work_file = malloc(sizeof(work_file_t));
        assert(work_file != NULL);
        // populate fields within the work_file structure and add it to the queue
        work_file->index = extract_index(files[i]); // extract index from file name
        work_file->directory = directory;
        work_file->filename = files[i];
        enqueue_work(&pool, work_file);
    }


    // initialize the array that stores compressed file data
    for (i = 0; i < nfiles; i++) {
        compressed_files[i].size = 0;
    }

    // delete the thread pool after all compression work is done
    thread_pool_delete(&pool);

    // create a single zipped package called video.vzip with all ppm files in lexicographical order
    FILE *f_out = fopen("video.vzip", "wb");    // open a file stream for writing
    assert(f_out != NULL);

    // for each compressed file in the array, write out to the final video.vzip file
    for (i = 0; i < nfiles;  i++) {
        // proceed to sequentially write each compressed file to the output file
        fwrite(&compressed_files[i].size, sizeof(int), 1, f_out);
        fwrite(compressed_files[i].data, sizeof(unsigned char), compressed_files[i].size, f_out);
        total_in += compressed_files[i].input_size;
        total_out += compressed_files[i].size;
        free(compressed_files[i].data); // free compressed data from the array after writing    
    }

    // close the output file stream
    fclose(f_out);

    // print compression rate
    printf("Compression rate: %.2lf%%\n", 100.0 * (total_in - total_out) / total_in);

    // release the compressed files array
    free(compressed_files);

    // release list of files
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

/***********************************************************************
 * function worker_thread() takes in any type of argument, and returns *
 * any type. This routine is the primary routine that worker threads   *
 * in the thread pool will be assigned. It consists of a while loop    *
 * that forces all worker threads to continuously grab work_files from *
 * the queue and compress it until there are no more work files to be  *
 * be compressed.                                                      *
 ***********************************************************************/
void *worker_thread(void *arg) {
    // cast the argument into a pointer to the thread pool
    thread_pool_t *pool = (thread_pool_t *) arg;
    // break loop and finish routine only when no more work files to be processed
    while (1) {
        // use mutexes to ensure two threads don't work on compressing the same file
        pthread_mutex_lock(&pool->queue_mutex);
        // wait while the work queue is empty and the stop indicator is still off
        while (pool->work_queue_head == NULL && !pool->stop) {
            pthread_cond_wait(&pool->queue_cond, &pool->queue_mutex);
        }
        // if stop indicator is on and work queue is empty, all work is done, so unlock mutex and exit
        if (pool->stop && pool->work_queue_head == NULL) {
            pthread_mutex_unlock(&pool->queue_mutex);
            break;
        }
        // otherwise, continue with processing and obtain next work item
        work_file_t *work_file = dequeue_work(pool);
        pthread_mutex_unlock(&pool->queue_mutex); // release lock - not needed until we pull next item

        // now thread begins to execute the compression of the file pulled from the work queue
        if (work_file) {
            int index = work_file->index;
            char *filename = work_file->filename;

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

            // Store the size of the input file to the compressed files array
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

            // Store the compressed data into the globally defined compressed files array
            int nbytes_zipped = BUFFER_SIZE - strm.avail_out;
            compressed_files[index].data = malloc(nbytes_zipped * sizeof(unsigned char));
            assert(compressed_files[index].data != NULL);
            memcpy(compressed_files[index].data, buffer_out, nbytes_zipped);
            compressed_files[index].size = nbytes_zipped;

            // free all dynamically allocated pointers
            free(buffer_in);
            free(buffer_out);
            free(full_path);
            free(work_file); // free the completed work item
        }
    }
    return NULL;
}


/************************************************************************
 * function enqueue_work() takes in a pointer to the thread pool and    *
 * creates a new node to add to the end of the linked list representing *
 * the queue. While enqueuing the file, mutex is used to prevent orphan *
 * nodes, thus preventing concurrency issues.                           *
 ************************************************************************/
void enqueue_work(thread_pool_t *pool, work_file_t *work_file) {
    work_node_t *node = malloc(sizeof(work_node_t));
    assert(node != NULL);
    node->work_file = work_file;
    node->next = NULL;

    // add new node to the end of the linked list
    // concurrency lock needed here to prevent orphan nodes
    pthread_mutex_lock(&pool->queue_mutex);
    if (pool->work_queue_tail == NULL) {  // if empty queue head = tail = node
        pool->work_queue_head = node;
        pool->work_queue_tail = node;
    } else {
        pool->work_queue_tail->next = node;
        pool->work_queue_tail = node;
    }
    // send signal to a blocked thread that one more file is ready to be compressed
    pthread_cond_signal(&pool->queue_cond);
    pthread_mutex_unlock(&pool->queue_mutex);
}

/************************************************************************
 * function dequeue_work() takes in a pointer to the thread pool and    *
 * returns a pointer to the next work file to be processed.             * 
 ************************************************************************/
work_file_t *dequeue_work(thread_pool_t *pool) {
    work_node_t *node = pool->work_queue_head;
    if (node == NULL) { // if work queue is empty
        return NULL;
    }
    // the head pointer of linked list points to the subsequent work node
    pool->work_queue_head = node->next;
    if (pool->work_queue_head == NULL) {  // if resulting list is empty
        pool->work_queue_tail = NULL;
    }
    // return the work file stored inside the work node
    work_file_t *work_file = node->work_file;
    free(node);
    return work_file;
}

/************************************************************************
 * function thread_pool_init() takes in a pointer to the thread pool   *
 * and initilizes the thread pool object setting head and tail pointers *
 * to NULL to indicate an empty work queue. This function also          *
 * initializes the mutex and condition variables to be shared amongst   *
 * the thread to manage concurrency issues during processing.           *
 ************************************************************************/
void thread_pool_init(thread_pool_t *pool) {
    // initialize empty linked list for the queue
    pool->work_queue_head = NULL;
    pool->work_queue_tail = NULL;
    // stop flag is initially off (indicating outstanding work)
    pool->stop = 0;
    // intialize the mutex and condition variable
    pthread_mutex_init(&pool->queue_mutex, NULL);
    pthread_cond_init(&pool->queue_cond, NULL);
    // populate the threads array
	int i;
    for (i = 0; i < MAX_THREADS; i++) {
        pthread_create(&pool->threads[i], NULL, worker_thread, pool);
    }
}

/************************************************************************
 * function thread_pool_delete() cleans up the thread pool, unblocking  *
 * all threads and deleting the mutex and condition variable.           *
 ************************************************************************/
void thread_pool_delete(thread_pool_t *pool) {
    // define a critial section to mark that all work is done by setting the stop signal
    pthread_mutex_lock(&pool->queue_mutex);
    pool->stop = 1;
    pthread_cond_broadcast(&pool->queue_cond);
    pthread_mutex_unlock(&pool->queue_mutex);

    //for each thread, join it with the main
	int i;
    for (i = 0; i < MAX_THREADS; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    // delete the mutex and condition variable
    pthread_mutex_destroy(&pool->queue_mutex);
    pthread_cond_destroy(&pool->queue_cond);
}

/************************************************************************
 * function extract_index() takes in the .ppm file name, and uses the   *
 * the part of the name before the extension to determine the frame     *
 * number. This index is used to store the compressed file in the       *
 * proper position within the globally allocated compressed files array * 
 ************************************************************************/
int extract_index(const char *filename){
    // find lenght of the filename
    int len = strlen(filename);
    int index;
    // store the part of the filename before the extension
    char *tmp = (char *) malloc(sizeof(char)*5);
    strncpy(tmp, filename, len - 4);
    // convert string to an integer, subtracting one to use with array indexing
    index = atoi(tmp) - 1;
    free(tmp);
    return index;
}