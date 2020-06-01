//
// Created by czl on 4/18/20.
//
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "tracer1.h"
#include <libmemcached/memcached.h>

#define BATCH_SIZE 4096
#define SET_HEAD_SIZE 32
#define GET_HEAD_SIZE 24

pthread_mutex_t printmutex;

/* create a memcached structure */
static memcached_st *memc_new()
{
    char config_string[1024];
    memcached_st *memc = NULL;
    unsigned long long getter;

    pthread_mutex_lock (&printmutex);
    sprintf(config_string, "--SERVER=%s --BINARY-PROTOCOL", "127.0.0.1");
   // printf("config_string = %s\n", config_string);
    memc = memcached(config_string, strlen(config_string));

    getter = memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_NO_BLOCK);
    //printf("No block: %lld\n", getter);
    getter = memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_SOCKET_SEND_SIZE);
    //printf("Socket send size: %lld\n", getter);
    getter = memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_SOCKET_RECV_SIZE);
    //printf("Socket recv size: %lld\n", getter);

    pthread_mutex_unlock (&printmutex);
    return memc;
}


static int memc_put(memcached_st *memc, char *key,int key_len, char *val,int val_len) {
    memcached_return_t rc;
    rc = memcached_set(memc, key, key_len, val, val_len, (time_t) 0, (uint32_t) 0);
    if (rc != MEMCACHED_SUCCESS) {
        return 1;
    }
    return 0;
}

/* wrapper of get command */
static char* memc_get(memcached_st *memc, char *key,int key_len) {
    memcached_return_t rc;
    char *val;
    size_t len;
    uint32_t flag;
    val = memcached_get(memc, key, key_len, &len, &flag, &rc);
    if (rc != MEMCACHED_SUCCESS) {
        return NULL;
    }
    return val;
}


using namespace ycsb;

typedef struct SendBatch{
    char* buf;
    std::size_t size;//real data size
}sendbatch;

int THREAD_NUM=4;

unsigned long  *runtimelist;

std::vector<YCSB_request *> loads;

std::vector<sendbatch> database;

void con_database(int begin_index=0,int end_index=loads.size());

void con_package(char *buf,YCSB_request * req);

int get_package_size(YCSB_request * req);


void send_thread(int tid);

int main(int argc, char **argv){
    char * path;
    if(argc == 3){
        THREAD_NUM=std::atol(argv[1]);
        runtimelist=(unsigned long *)calloc(sizeof(unsigned long),THREAD_NUM);
        path=argv[2];
    }else{
        printf("please input filename\n");
        return 0;
    }

    int key_range=200;
    YCSBLoader loader( path);
    loads=loader.load();
    int size=loader.size();
    //con_database();

    std::vector<std::thread> threads;
    int send_batch_num=database.size()/THREAD_NUM;
    for(int i=0;i<THREAD_NUM;i++){
        printf("creating thread %d\n",i);
        threads.push_back(std::thread(send_thread,i));
    }
    for(int i=0;i<THREAD_NUM;i++){
        printf("stoping %d\n",i);
        threads[i].join();
    }

    //calculate running time

    unsigned long runtime=0;
    for(int i = 0;i < THREAD_NUM; i++){
        runtime += runtimelist[i];
    }
    runtime /= (THREAD_NUM);
    printf("\n____\nruntime:%lu\n",runtime);

    return 0;
}

void send_thread(int tid){
    int send_num = loads.size()/THREAD_NUM;
    int start_index = tid*send_num;


    memcached_st *memc;
    memc = memc_new();
    Tracer tracer;
    tracer.startTime();
    int buf_len=8;
    char buf[buf_len];
    int hit =0,miss=0,in=0;
    for (int i = start_index ;i <start_index+send_num ; i++) {
        YCSB_request * req=loads[i];
        if (req->getOp()==lookup) {
            memc_put(memc, req->getKey(),req->keyLength(), req->getVal(),req->valLength());
            in++;
        } else if (req->getOp()==insert||req->getOp()==update) {
            char *val = memc_get(memc, req->getKey(),req->keyLength());
            if (val == NULL) {
                // cache miss, put something (gabage) in cache
                miss++;
                memc_put(memc, req->getKey(), req->keyLength(),buf,buf_len);
            } else {
                hit++;
                free(val);
            }
        } else {
            fprintf(stderr, "unknown query type\n");
        }
    }

    runtimelist[tid]+=tracer.getRunTime();
    printf("thread %d insert%d hit %d miss %d\n",tid,in,hit,miss);
//    close(connect_fd);
}

void con_database(int begin_index,int end_index){
    int offset=0; //package offset
    sendbatch  batch ;
    batch.buf=(char *)malloc(BATCH_SIZE);
    memset(batch.buf,0,BATCH_SIZE);
    for(int i= begin_index;i<end_index;i++){
        YCSB_request * req=loads[i];
        if(offset+get_package_size(req)>BATCH_SIZE){
            batch.size=offset;
            database.push_back(batch);

            sendbatch newbatch;
            newbatch.buf=(char *)malloc(BATCH_SIZE);
            memset(newbatch.buf,0,BATCH_SIZE);
            batch=newbatch;

            offset=0;
            i--;
            continue;
        }else{
            con_package(batch.buf+offset,req);
            offset+=get_package_size(req);
        }
    }
    batch.size=offset;
    database.push_back(batch);

}

int get_package_size(YCSB_request * req){
    if(req->getOp()==lookup){
       // return 24+req->keyLength();
        return 24+8;
    }else if(req->getOp()==insert||req->getOp()==update){
        //return 32+req->keyLength()+req->valLength();
        return 32+8+req->valLength();
    } else{
        return BATCH_SIZE;
    }
}


void con_package(char *buf,YCSB_request * req){
    if(req->getOp()==lookup){
        //GETQ
        buf[0]=0x80;
        buf[1]=0x09;
       // size_t ks=req->keyLength();
       size_t ks =8;
        *(unsigned short int *)(buf+2)= htons((unsigned short int)ks);
        *(unsigned int *)(buf+8)=htonl((unsigned int)ks);
        //memcpy(buf+24,req->getKey(),ks);
        unsigned long int tmp=std::atol(req->getKey());
        *(unsigned long *)(buf+24)=tmp;
    }else if(req->getOp()==insert||req->getOp()==update){
        //SET
        buf[0]=0x80;
        buf[1]=0x11;
        //size_t ks=req->keyLength();
        size_t ks=8;
        size_t vs=req->valLength();
        *(unsigned short int *)(buf+2)=htons((unsigned short int)ks);
        buf[4]=0x08;
        *(unsigned int *)(buf+8)=htonl((unsigned int)(ks+vs+8));
        unsigned long int tmp=std::atol(req->getKey());
        *(unsigned long *)(buf+32)=tmp;
        //memcpy(buf+32,req->getKey(),ks);
        mempcpy(buf+32+ks,req->getVal(),vs);
    }
}

