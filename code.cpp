// pc_student_v2.c — Student-friendly Producer/Consumer with priority + metrics
// ---------------------------------------------------------------------------
// Build:  gcc -Wall -pthread -o pc_student_v2 pc_student_v2.c
// Run:    ./pc_student_v2 P C BUF [ITEMS=20] [URG_PCT=25]
// Example: ./pc_student_v2 3 2 10

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include <limits.h>
#include <stdint.h>

#define POISON_PILL INT_MIN

// ---------------------------------------------------------------------------
// Global variables and shared state
// ---------------------------------------------------------------------------

static int *Q_URG = NULL;   // urgent queue
static int *Q_NRM = NULL;   // normal queue

static int CAP = 0;         // total buffer capacity

// Renamed variables for clarity
static int urgent_head=0, urgent_tail=0, urgent_size=0;
static int normal_head=0, normal_tail=0, normal_size=0;

static sem_t sem_empty;     // empty slots
static sem_t sem_full;      // filled slots
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static int P=0, C=0;        // producers, consumers
static int ITEMS=20;        // items per producer
static int URG_PCT=25;      // % urgent items

// Metrics
static long long total_real=0, produced_real=0, consumed_real=0;
static long double sum_latency_ns=0;
static struct timespec Tstart, Tend;

static struct timespec *TURG=NULL, *TNRM=NULL;

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

static long double diff_ns(struct timespec a, struct timespec b){
    return ((long double)(a.tv_sec - b.tv_sec))*1.0e9L +
           ((long double)(a.tv_nsec - b.tv_nsec));
}

static void push_urgent(int v){
    Q_URG[urgent_tail] = v;
    clock_gettime(CLOCK_MONOTONIC, &TURG[urgent_tail]);
    urgent_tail = (urgent_tail + 1) % CAP;
    urgent_size++;
}

static void push_normal(int v){
    Q_NRM[normal_tail] = v;
    clock_gettime(CLOCK_MONOTONIC, &TNRM[normal_tail]);
    normal_tail = (normal_tail + 1) % CAP;
    normal_size++;
}

static int pop_urgent(struct timespec *tenq){
    int v = Q_URG[urgent_head];
    *tenq = TURG[urgent_head];
    urgent_head = (urgent_head + 1) % CAP;
    urgent_size--;
    return v;
}

static int pop_normal(struct timespec *tenq){
    int v = Q_NRM[normal_head];
    *tenq = TNRM[normal_head];
    normal_head = (normal_head + 1) % CAP;
    normal_size--;
    return v;
}

// ---------------------------------------------------------------------------
// Producer thread
// ---------------------------------------------------------------------------

static void *producer(void *arg){
    long id = (long)(intptr_t)arg;
    unsigned seed = (unsigned)(time(NULL) ^ (id*2654435761u));

    for(int k=0;k<ITEMS;k++){
        int value = rand_r(&seed) % 1000;
        int isUrg = (rand_r(&seed) % 100) < URG_PCT;

        sem_wait(&sem_empty);
        pthread_mutex_lock(&mutex);

        if(isUrg){
            if(urgent_size < CAP) push_urgent(value);
            else push_normal(value);
        } else {
            if(normal_size < CAP) push_normal(value);
            else push_urgent(value);
        }

        produced_real++;
        printf("Producer %ld -> produced item %d [%s]\n",
               id, value, isUrg?"URGENT":"normal");

        pthread_mutex_unlock(&mutex);
        sem_post(&sem_full);
    }

    return NULL;
}

// ---------------------------------------------------------------------------
// Consumer thread
// ---------------------------------------------------------------------------

static void *consumer(void *arg){
    long id = (long)(intptr_t)arg;

    for(;;){
        sem_wait(&sem_full);
        pthread_mutex_lock(&mutex);

        int v; struct timespec tenq, tnow;

        if(urgent_size > 0)
            v = pop_urgent(&tenq);
        else
            v = pop_normal(&tenq);

        clock_gettime(CLOCK_MONOTONIC, &tnow);

        if(v == POISON_PILL){
            printf("Consumer %ld <- received poison pill, exiting.\n", id);
            pthread_mutex_unlock(&mutex);
            sem_post(&sem_empty);
            break;
        }

        consumed_real++;
        long double ns = diff_ns(tnow, tenq);
        sum_latency_ns += ns;

        printf("Consumer %ld <- consumed item %d [latency: %.2Lf ms]\n",
               id, v, ns/1.0e6L);

        pthread_mutex_unlock(&mutex);
        sem_post(&sem_empty);
    }

    return NULL;
}

// ---------------------------------------------------------------------------
// Main function
// ---------------------------------------------------------------------------

int main(int argc,char **argv){
    if(argc < 4){
        fprintf(stderr,"Usage: %s P C BUF [ITEMS=20] [URG_PCT=25]\n",argv[0]);
        return 1;
    }

    P = atoi(argv[1]);
    C = atoi(argv[2]);
    CAP = atoi(argv[3]);
    if(argc >= 5) ITEMS = atoi(argv[4]);
    if(argc >= 6) URG_PCT = atoi(argv[5]);

    if(P<=0 || C<=0 || CAP<=0 || ITEMS<=0 || URG_PCT<0 || URG_PCT>100){
        fprintf(stderr,"Invalid arguments.\n");
        return 1;
    }

    total_real = (long long)P * (long long)ITEMS;

    Q_URG = calloc(CAP, sizeof(int));
    Q_NRM = calloc(CAP, sizeof(int));
    TURG  = calloc(CAP, sizeof(struct timespec));
    TNRM  = calloc(CAP, sizeof(struct timespec));

    if(!Q_URG || !Q_NRM || !TURG || !TNRM){
        fprintf(stderr,"Memory allocation failed.\n");
        return 1;
    }

    sem_init(&sem_empty,0,CAP);
    sem_init(&sem_full,0,0);
    pthread_mutex_init(&mutex,NULL);

    pthread_t *prod = calloc(P, sizeof(pthread_t));
    pthread_t *cons = calloc(C, sizeof(pthread_t));

    clock_gettime(CLOCK_MONOTONIC,&Tstart);

    for(long i=0;i<P;i++)
        pthread_create(&prod[i],NULL,producer,(void*)(intptr_t)(i+1));

    for(long j=0;j<C;j++)
        pthread_create(&cons[j],NULL,consumer,(void*)(intptr_t)(j+1));

    for(int i=0;i<P;i++) pthread_join(prod[i],NULL);

    // Send poison pills
    for(int j=0;j<C;j++){
        sem_wait(&sem_empty);
        pthread_mutex_lock(&mutex);

        if(normal_size < CAP)
            push_normal(POISON_PILL);
        else
            push_urgent(POISON_PILL);

        pthread_mutex_unlock(&mutex);
        sem_post(&sem_full);
    }

    for(int j=0;j<C;j++) pthread_join(cons[j],NULL);

    clock_gettime(CLOCK_MONOTONIC,&Tend);

    long double wall_s = diff_ns(Tend,Tstart)/1.0e9L;
    long double avg_ms = (consumed_real>0)
                         ? (sum_latency_ns/(long double)consumed_real)/1.0e6L
                         : 0.0L;
    long double thr = (wall_s>0.0L)
                      ? ((long double)consumed_real / wall_s)
                      : 0.0L;

    printf("\n=== Summary ===\n");
    printf("Produced: %lld | Consumed: %lld\n", total_real, consumed_real);
    printf("Average Latency: %.3Lf ms | Throughput: %.2Lf items/s | Time: %.3Lf s\n",
           avg_ms, thr, wall_s);

    free(prod); free(cons);
    free(Q_URG); free(Q_NRM);
    free(TURG); free(TNRM);
    sem_destroy(&sem_empty);
    sem_destroy(&sem_full);
    pthread_mutex_destroy(&mutex);

    return 0;
}
