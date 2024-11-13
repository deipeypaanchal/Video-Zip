// #include <dirent.h> 
// #include <stdio.h> 
// #include <assert.h>
// #include <stdlib.h>
// #include <string.h>
// #include <zlib.h>
// #include <time.h>

// #define BUFFER_SIZE 1048576 // 1MB

// int cmp(const void *a, const void *b) {
// 	return strcmp(*(char **) a, *(char **) b);
// }

// int main(int argc, char **argv) {
// 	// time computation header
// 	struct timespec start, end;
// 	clock_gettime(CLOCK_MONOTONIC, &start);
// 	// end of time computation header

// 	// do not modify the main function before this point!

// 	assert(argc == 2);

// 	DIR *d;
// 	struct dirent *dir;
// 	char **files = NULL;
// 	int nfiles = 0;

// 	d = opendir(argv[1]);
// 	if(d == NULL) {
// 		printf("An error has occurred\n");
// 		return 0;
// 	}

// 	// create sorted list of PPM files
// 	while ((dir = readdir(d)) != NULL) {
// 		files = realloc(files, (nfiles+1)*sizeof(char *));
// 		assert(files != NULL);

// 		int len = strlen(dir->d_name);
// 		if(dir->d_name[len-4] == '.' && dir->d_name[len-3] == 'p' && dir->d_name[len-2] == 'p' && dir->d_name[len-1] == 'm') {
// 			files[nfiles] = strdup(dir->d_name);
// 			assert(files[nfiles] != NULL);

// 			nfiles++;
// 		}
// 	}
// 	closedir(d);
// 	qsort(files, nfiles, sizeof(char *), cmp);

// 	// create a single zipped package with all PPM files in lexicographical order
// 	int total_in = 0, total_out = 0;
// 	FILE *f_out = fopen("video.vzip", "w");
// 	assert(f_out != NULL);
// 	int i = 0;
// 	for(i=0; i < nfiles; i++) {
// 		int len = strlen(argv[1])+strlen(files[i])+2;
// 		char *full_path = malloc(len*sizeof(char));
// 		assert(full_path != NULL);
// 		strcpy(full_path, argv[1]);
// 		strcat(full_path, "/");
// 		strcat(full_path, files[i]);

// 		unsigned char buffer_in[BUFFER_SIZE];
// 		unsigned char buffer_out[BUFFER_SIZE];

// 		// load file
// 		FILE *f_in = fopen(full_path, "r");
// 		assert(f_in != NULL);
// 		int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
// 		fclose(f_in);
// 		total_in += nbytes;

// 		// zip file
// 		z_stream strm;
// 		int ret = deflateInit(&strm, 9);
// 		assert(ret == Z_OK);
// 		strm.avail_in = nbytes;
// 		strm.next_in = buffer_in;
// 		strm.avail_out = BUFFER_SIZE;
// 		strm.next_out = buffer_out;

// 		ret = deflate(&strm, Z_FINISH);
// 		assert(ret == Z_STREAM_END);

// 		// dump zipped file
// 		int nbytes_zipped = BUFFER_SIZE-strm.avail_out;
// 		fwrite(&nbytes_zipped, sizeof(int), 1, f_out);
// 		fwrite(buffer_out, sizeof(unsigned char), nbytes_zipped, f_out);
// 		total_out += nbytes_zipped;

// 		free(full_path);
// 	}
// 	fclose(f_out);

// 	printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/total_in);

// 	// release list of files
// 	for(i=0; i < nfiles; i++)
// 		free(files[i]);
// 	free(files);

// 	// do not modify the main function after this point!

// 	// time computation footer
// 	clock_gettime(CLOCK_MONOTONIC, &end);
// 	printf("Time: %.2f seconds\n", ((double)end.tv_sec+1.0e-9*end.tv_nsec)-((double)start.tv_sec+1.0e-9*start.tv_nsec));
// 	// end of time computation footer

// 	return 0;
// }

// VERSION 2
/*
#include <dirent.h> 
#include <stdio.h> 
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <time.h>
#include <pthread.h>

#define BUFFER_SIZE 1048576 // 1MB
#define NUM_THREADS 20  // Reduced number of threads for stability

// Structure to hold compression data
typedef struct {
    unsigned char *compressed_data;
    int compressed_size;
    int input_size;
} CompressedFile;

// Structure for thread arguments
typedef struct {
    char *dirname;
    char **filenames;
    int start_idx;
    int end_idx;
    CompressedFile *results;
} ThreadArgs;

int cmp(const void *a, const void *b) {
    return strcmp(*(char **) a, *(char **) b);
}

void* compress_chunk(void *arg) {
    ThreadArgs *args = (ThreadArgs*)arg;
    
    for (int i = args->start_idx; i < args->end_idx; i++) {
        // Construct full path
        char *full_path = malloc(strlen(args->dirname) + strlen(args->filenames[i]) + 2);
        if (!full_path) continue;
        
        sprintf(full_path, "%s/%s", args->dirname, args->filenames[i]);
        
        // Read input file
        FILE *f_in = fopen(full_path, "r");
        if (!f_in) {
            free(full_path);
            continue;
        }
        
        // Allocate buffers
        unsigned char *buffer_in = malloc(BUFFER_SIZE);
        unsigned char *buffer_out = malloc(BUFFER_SIZE);
        
        if (!buffer_in || !buffer_out) {
            free(buffer_in);
            free(buffer_out);
            free(full_path);
            fclose(f_in);
            continue;
        }
        
        // Read file content
        int nbytes = fread(buffer_in, 1, BUFFER_SIZE, f_in);
        fclose(f_in);
        
        // Initialize zlib stream
        z_stream strm = {0};
        if (deflateInit(&strm, 9) != Z_OK) {
            free(buffer_in);
            free(buffer_out);
            free(full_path);
            continue;
        }
        
        // Set up compression
        strm.avail_in = nbytes;
        strm.next_in = buffer_in;
        strm.avail_out = BUFFER_SIZE;
        strm.next_out = buffer_out;
        
        // Compress
        int ret = deflate(&strm, Z_FINISH);
        if (ret == Z_STREAM_END) {
            args->results[i].compressed_data = buffer_out;
            args->results[i].compressed_size = BUFFER_SIZE - strm.avail_out;
            args->results[i].input_size = nbytes;
        } else {
            free(buffer_out);
        }
        
        // Cleanup
        deflateEnd(&strm);
        free(buffer_in);
        free(full_path);
    }
    
    return NULL;
}

int main(int argc, char **argv) {
    // time computation header
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    assert(argc == 2);

    DIR *d;
    struct dirent *dir;
    char **files = NULL;
    int nfiles = 0;

    d = opendir(argv[1]);
    if(d == NULL) {
        printf("An error has occurred\n");
        return 0;
    }

    // Create sorted list of PPM files
    while ((dir = readdir(d)) != NULL) {
        int len = strlen(dir->d_name);
        if (len > 4 && strcmp(dir->d_name + len - 4, ".ppm") == 0) {
            files = realloc(files, (nfiles + 1) * sizeof(char *));
            if (!files) {
                printf("Memory allocation error\n");
                closedir(d);
                return 0;
            }
            
            files[nfiles] = strdup(dir->d_name);
            if (!files[nfiles]) {
                printf("Memory allocation error\n");
                closedir(d);
                return 0;
            }
            
            nfiles++;
        }
    }
    closedir(d);
    
    if (nfiles == 0) {
        printf("No PPM files found\n");
        return 0;
    }
    
    qsort(files, nfiles, sizeof(char *), cmp);

    // Initialize compression results array
    CompressedFile *results = calloc(nfiles, sizeof(CompressedFile));
    if (!results) {
        printf("Memory allocation error\n");
        return 0;
    }

    // Create threads
    pthread_t threads[NUM_THREADS];
    ThreadArgs thread_args[NUM_THREADS];
    int files_per_thread = (nfiles + NUM_THREADS - 1) / NUM_THREADS;

    for (int i = 0; i < NUM_THREADS; i++) {
        thread_args[i].dirname = argv[1];
        thread_args[i].filenames = files;
        thread_args[i].start_idx = i * files_per_thread;
        thread_args[i].end_idx = (i + 1) * files_per_thread;
        if (thread_args[i].end_idx > nfiles) {
            thread_args[i].end_idx = nfiles;
        }
        thread_args[i].results = results;

        if (thread_args[i].start_idx >= nfiles) {
            break;
        }

        if (pthread_create(&threads[i], NULL, compress_chunk, &thread_args[i]) != 0) {
            printf("Thread creation error\n");
            return 0;
        }
    }

    // Wait for threads to complete
    for (int i = 0; i < NUM_THREADS; i++) {
        if (i * files_per_thread < nfiles) {
            pthread_join(threads[i], NULL);
        }
    }

    // Write compressed data
    FILE *f_out = fopen("video.vzip", "w");
    if (!f_out) {
        printf("Cannot create output file\n");
        return 0;
    }

    int total_in = 0, total_out = 0;
    for (int i = 0; i < nfiles; i++) {
        if (results[i].compressed_data) {
            fwrite(&results[i].compressed_size, sizeof(int), 1, f_out);
            fwrite(results[i].compressed_data, 1, results[i].compressed_size, f_out);
            total_in += results[i].input_size;
            total_out += results[i].compressed_size;
            free(results[i].compressed_data);
        }
    }
    fclose(f_out);

    printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/total_in);

    // Cleanup
    for (int i = 0; i < nfiles; i++) {
        free(files[i]);
    }
    free(files);
    free(results);

    // time computation footer
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Time: %.2f seconds\n", ((double)end.tv_sec+1.0e-9*end.tv_nsec)-((double)start.tv_sec+1.0e-9*start.tv_nsec));

    return 0;
}
*/

#include <dirent.h>
#include <stdio.h> 
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <time.h>
#include <pthread.h>

#define BUFFER_SIZE 1048576 // 1MB
#define MAX_THREADS 20      // Maximum number of worker threads

/**
 * Structure to store compressed file data.
 */
typedef struct {
    int input_size;            // Size of the input file
    int size;                  // Size of the compressed data
    unsigned char *data;       // Compressed data
} compressed_file_t;

/**
 * Structure to pass work items to the worker threads.
 */
typedef struct {
    int index;                     // Index of the file in the files array
    char *directory;               // Directory containing the files
    char *filename;                // Filename to process
} work_item_t;

/**
 * Queue node structure.
 */
typedef struct work_node {
    work_item_t *work_item;
    struct work_node *next;
} work_node_t;

/**
 * Thread pool structure.
 */
typedef struct {
    pthread_t threads[MAX_THREADS];
    work_node_t *work_queue_head;
    work_node_t *work_queue_tail;
    pthread_mutex_t queue_mutex;
    pthread_cond_t queue_cond;
    int stop;
} thread_pool_t;

// Global variables
compressed_file_t *compressed_files;
int total_in = 0, total_out = 0;
char **files = NULL;
int nfiles = 0;
char *directory;

/**
 * Function prototypes.
 */
void *worker_thread(void *arg);
void enqueue_work(thread_pool_t *pool, work_item_t *work_item);
work_item_t *dequeue_work(thread_pool_t *pool);
void thread_pool_init(thread_pool_t *pool);
void thread_pool_destroy(thread_pool_t *pool);
void *worker_thread(void *arg);

/**
 * Comparator function for qsort to sort filenames lexicographically.
 */
int cmp(const void *a, const void *b) {
    return strcmp(*(char **) a, *(char **) b);
}

/**
 * Worker thread function to process files.
 */
void *worker_thread(void *arg) {
    thread_pool_t *pool = (thread_pool_t *)arg;
    while (1) {
        pthread_mutex_lock(&pool->queue_mutex);
        while (pool->work_queue_head == NULL && !pool->stop) {
            pthread_cond_wait(&pool->queue_cond, &pool->queue_mutex);
        }
        if (pool->stop && pool->work_queue_head == NULL) {
            pthread_mutex_unlock(&pool->queue_mutex);
            break;
        }
        work_item_t *work_item = dequeue_work(pool);
        pthread_mutex_unlock(&pool->queue_mutex);

        if (work_item) {
            int index = work_item->index;
            char *filename = work_item->filename;

            // Build the full path to the file
            int len = strlen(directory) + strlen(filename) + 2;
            char *full_path = malloc(len * sizeof(char));
            assert(full_path != NULL);
            strcpy(full_path, directory);
            strcat(full_path, "/");
            strcat(full_path, filename);

            // Allocate buffers on the heap
            unsigned char *buffer_in = malloc(BUFFER_SIZE * sizeof(unsigned char));
            assert(buffer_in != NULL);
            unsigned char *buffer_out = malloc(BUFFER_SIZE * sizeof(unsigned char));
            assert(buffer_out != NULL);

            // Load the input file
            FILE *f_in = fopen(full_path, "rb");
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

            // Store the compressed data
            int nbytes_zipped = BUFFER_SIZE - strm.avail_out;
            compressed_files[index].data = malloc(nbytes_zipped * sizeof(unsigned char));
            assert(compressed_files[index].data != NULL);
            memcpy(compressed_files[index].data, buffer_out, nbytes_zipped);
            compressed_files[index].size = nbytes_zipped;

            // Clean up
            free(buffer_in);
            free(buffer_out);
            free(full_path);
            free(work_item); // Free the work item
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

    pthread_mutex_lock(&pool->queue_mutex);
    if (pool->work_queue_tail == NULL) {
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
    if (pool->work_queue_head == NULL) {
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
    for (int i = 0; i < MAX_THREADS; i++) {
        pthread_create(&pool->threads[i], NULL, worker_thread, pool);
    }
}

/**
 * Destroy the thread pool.
 */
void thread_pool_destroy(thread_pool_t *pool) {
    pthread_mutex_lock(&pool->queue_mutex);
    pool->stop = 1;
    pthread_cond_broadcast(&pool->queue_cond);
    pthread_mutex_unlock(&pool->queue_mutex);
    for (int i = 0; i < MAX_THREADS; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    pthread_mutex_destroy(&pool->queue_mutex);
    pthread_cond_destroy(&pool->queue_cond);
}

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

    // Create sorted list of PPM files
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
    for (int i = 0; i < nfiles; i++) {
        compressed_files[i].input_size = 0;
        compressed_files[i].size = 0;
        compressed_files[i].data = NULL;
    }

    // Initialize thread pool
    thread_pool_t pool;
    thread_pool_init(&pool);

    // Enqueue work items
    for (int i = 0; i < nfiles; i++) {
        work_item_t *work_item = malloc(sizeof(work_item_t));
        assert(work_item != NULL);
        work_item->index = i;
        work_item->directory = directory;
        work_item->filename = files[i];
        enqueue_work(&pool, work_item);
    }

    // Destroy thread pool after all work is done
    thread_pool_destroy(&pool);

    // Create a single zipped package with all PPM files in lexicographical order
    FILE *f_out = fopen("video.vzip", "wb");
    assert(f_out != NULL);

    for (int i = 0; i < nfiles; i++) {
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
    for (int i = 0; i < nfiles; i++)
        free(files[i]);
    free(files);

    // Do not modify the main function after this point!

    // Time computation footer
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Time: %.2f seconds\n", ((double)end.tv_sec + 1.0e-9 * end.tv_nsec) - ((double)start.tv_sec + 1.0e-9 * start.tv_nsec));
    // End of time computation footer

    return 0;
}