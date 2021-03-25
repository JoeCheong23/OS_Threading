/*
    The Hybrid Merge Sort to use for Operating Systems Assignment 1 2021
    written by Robert Sheehan

    Modified by: Joe Cheong
    UPI: jche545

    By submitting a program you are claiming that you and only you have made
    adjustments and additions to this code.
 */

#include <stdio.h> 
#include <stdlib.h> 
#include <unistd.h> 
#include <string.h>
#include <sys/resource.h>
#include <stdbool.h>
#include <sys/time.h>
#include <sys/times.h>
#include <math.h>
#include <pthread.h>
#include <semaphore.h>

#define SIZE    4
#define MAX     1000
#define SPLIT   16
#define MAX_THREADS 8

int thread_num = 1;
bool create_new_thread = true;
pthread_mutex_t lock;
pthread_cond_t wake_signal = PTHREAD_COND_INITIALIZER;
struct threadpool threadpool;

void* thread_function(void *arg);

typedef struct {
    pthread_t thread; 

} thread_structure; 

typedef struct {
    void* (*function)(void *); //function to be executed
    void* argument; //argument for the function
    sem_t job_completion_signal; //semaphore that the child thread will post to when it has completed it's task so the parent knows when to call merge.
} job;

struct threadpool {
    thread_structure thread_array[MAX_THREADS-1];
    job queue[10];
    int available_threads; //number of threads that are currently waiting
    int head_pointer; //array position to pop jobs to execute.
    int tail_pointer; //array position to push new jobs to the queue.
    int queue_size;
    bool program_complete;

};

struct block {
    int size;
    int *data;
};

struct split_pointer {
    int *left_pointer;
    int *right_pointer;
};

void initialise_threadpool() {


    //initialise and add all threads into the thread_array.
    for (int i = 0; i < MAX_THREADS-1; i++) {

        pthread_t thread;
        thread_structure threadStructure;
        pthread_create(&thread, NULL, thread_function, NULL); //create and initialise the thread
        threadStructure.thread = thread;
        threadpool.thread_array[i] = threadStructure;

    }

    //initialise the jobs in the job queue, including providing each job struct with its own semaphore.
    for (int i = 0; i < 10; i++) {

        sem_t job_semaphore;
        job job_instance;
        job_instance.function = NULL;
        job_instance.argument = NULL;
        sem_init(&job_semaphore, 1, 0); //initialise semaphore with value 0 so when parent calls sem_wait, it will block till threadpool thread completes the task.

        job_instance.job_completion_signal = job_semaphore;
        threadpool.queue[i] = job_instance;
    
    }

    threadpool.available_threads = 7;
    threadpool.head_pointer = 0;
    threadpool.tail_pointer = 0;
    threadpool.queue_size = 0;
    threadpool.program_complete = false;

}

void* thread_function(void *arg) {

    job current_job;
    int head_pointer;

    while (true) {

        pthread_mutex_lock(&lock);

        while ((threadpool.queue_size == 0) && (!threadpool.program_complete)) {
            printf("Thread waiting");
            pthread_cond_wait(&wake_signal, &lock);
            printf("Signal received");
        }

        if (threadpool.program_complete) {
            pthread_mutex_unlock(&lock);
            break;
        }

        head_pointer = threadpool.head_pointer; //retrieve the array location of the first job in the queue.
        threadpool.head_pointer = (threadpool.head_pointer + 1) % 10; //increment the pointer to the next position in the array.
        current_job = threadpool.queue[head_pointer]; //retrieve the first job in the queue to be executed.
        threadpool.queue_size--; //reduce the size of the queue as one of the jobs has been removed.
        threadpool.available_threads--;

        pthread_mutex_unlock(&lock);

        (*(current_job.function))(current_job.argument);

        sem_post(&current_job.job_completion_signal);

        pthread_mutex_lock(&lock);
        threadpool.available_threads++; //job complete so the thread is available again.
        pthread_mutex_unlock(&lock);

    }

    pthread_exit(NULL);


}

void print_data(struct block *block) {
    for (int i = 0; i < block->size; ++i)
        printf("%d ", block->data[i]);
    printf("\n");
}

/* The insertion sort for smaller halves. */
void insertion_sort(struct block *block) {
    for (int i = 1; i < block->size; ++i) {
        for (int j = i; j > 0; --j) {
            if (block->data[j-1] > block->data[j]) {
                int temp;
                temp = block->data[j-1];
                block->data[j-1] = block->data[j];
                block->data[j] = temp;
            }
        }
    }
}

/* Combine the two halves back together. */
void merge(struct block *left, struct block *right) {
    int *combined = calloc(left->size + right->size, sizeof(int));
    if (combined == NULL) {
        perror("Allocating space for merge.\n");
        exit(EXIT_FAILURE);
    }
        int dest = 0, l = 0, r = 0;
        while (l < left->size && r < right->size) {
                if (left->data[l] < right->data[r])
                        combined[dest++] = left->data[l++];
                else
                        combined[dest++] = right->data[r++];
        }
        while (l < left->size)
                combined[dest++] = left->data[l++];
        while (r < right->size)
                combined[dest++] = right->data[r++];
    memmove(left->data, combined, (left->size + right->size) * sizeof(int));
    free(combined);
}

/* Merge sort the data. */
void* merge_sort(void *data_block) {

    struct block *block = (struct block*)data_block;

    if ((block->size > SPLIT)) {
        struct block left_block;
        struct block right_block;
        left_block.size = block->size / 2;
        left_block.data = block->data;
        right_block.size = block->size - left_block.size; // left_block.size + (block->size % 2);
        right_block.data = block->data + left_block.size;

        pthread_mutex_lock(&lock);

        if ((threadpool.available_threads > 0) && (threadpool.queue_size < 10)) {
            
            sem_t current_job_semaphore;

            int tail_pointer = threadpool.tail_pointer; //get position in job queue to add job to.
            threadpool.tail_pointer = (threadpool.tail_pointer + 1) % 10; //move pointer to next position for next time a job is to be added.
            threadpool.queue_size++; //increment the queue size as a job is going to be added to the queue.

            threadpool.queue[tail_pointer].function = merge_sort;
            threadpool.queue[tail_pointer].argument = &left_block;
            current_job_semaphore = threadpool.queue[tail_pointer].job_completion_signal;
            
            pthread_mutex_unlock(&lock);
            
            pthread_cond_broadcast(&wake_signal);
            
            merge_sort(&right_block);

            sem_wait(&current_job_semaphore);

            merge(&left_block, &right_block);


        } else {
            
            pthread_mutex_unlock(&lock);
            merge_sort(&left_block);
            merge_sort(&right_block);
            merge(&left_block, &right_block);

        }
    } else {
        insertion_sort(block);
    }
}

/* Check to see if the data is sorted. */
bool is_sorted(struct block *block) {
    bool sorted = true;
    for (int i = 0; i < block->size - 1; i++) {
        if (block->data[i] > block->data[i + 1])
            sorted = false;
    }
    return sorted;
}

/* Fill the array with random data. */
void produce_random_data(struct block *block) {
    srand(1); // the same random data seed every time
    for (int i = 0; i < block->size; i++) {
        block->data[i] = rand() % MAX;
    }
}

int main(int argc, char *argv[]) {


    long size;

    initialise_threadpool();

    if (argc < 2) {
            size = SIZE;
    } else {
            size = atol(argv[1]);
    }
    struct block block, left_block, right_block;
    block.size = (int)pow(2, size);
    block.data = (int *)calloc(block.size, sizeof(int));
    if (block.data == NULL) {
        perror("Unable to allocate space for data.\n");
        exit(EXIT_FAILURE);
    }

    produce_random_data(&block);

    struct timeval start_wall_time, finish_wall_time, wall_time;
    struct tms start_times, finish_times;
    gettimeofday(&start_wall_time, NULL);
    times(&start_times);

    merge_sort(&block);

    threadpool.program_complete = true;

    gettimeofday(&finish_wall_time, NULL);
    times(&finish_times);
    timersub(&finish_wall_time, &start_wall_time, &wall_time);
    printf("start time in clock ticks: %ld\n", start_times.tms_utime);
    printf("finish time in clock ticks: %ld\n", finish_times.tms_utime);
    printf("wall time %ld secs and %ld microseconds\n", wall_time.tv_sec, wall_time.tv_usec);

    if (block.size < 1025)
        print_data(&block);

    printf(is_sorted(&block) ? "sorted\n" : "not sorted\n");
    printf("Number of threads created: %d\n", thread_num);
    free(block.data);
    exit(EXIT_SUCCESS);
}