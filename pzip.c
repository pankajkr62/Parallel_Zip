#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/sysinfo.h>
#include <pthread.h>
#include <string.h>

int fileExists(char *name){
  FILE *fptr = fopen(name,"r");
  if(fptr == NULL){
      return 0;
  }
  fclose(fptr);
  return 1;
}
// this data is for each chunk
// and will be given to each thread
struct chunk {
  char* ptr;
  int start;
  int end;
};
 
// return data structure from threads
struct ret_Chunk {
   char   value[100000000];
   int    count[100000000];
   int    n;
};
//compress function for individual threads
// return and args are void due to pthread_create semantics
void* compress(void* args)
{
  struct ret_Chunk* compData = malloc(sizeof(struct ret_Chunk));
  compData->n = 0;
  char* ptr = ((struct chunk*)args)->ptr;
  int start = ((struct chunk*)args)->start;
  int end = ((struct chunk*)args)->end;
  //char prevChar = *(ptr + start);
  //char currentChar = *(ptr + start);
  char prevChar = 'a';
  char currentChar;
  int prevCharCount = 0;
  for(int i = start;i<=end;i++){
      currentChar = *(ptr+i);
      if(currentChar == '\0'){
          continue;
      }
      if(currentChar == '\n'){
        //continue;
      }
      if(currentChar == '/'){
          continue;
      }
      if(currentChar == ' '){
          continue;
      }
      if(prevCharCount == 0){
          // start the prevchar count
          prevChar = currentChar;
          prevCharCount = 1;
      }
      else{
          if(currentChar == prevChar){
              prevCharCount += 1;
          }
          else{
              // current char not equals to prevchar
              compData->value[compData->n] = prevChar;
              compData->count[compData->n] = prevCharCount;
              prevCharCount =1; // new char count will be 1 as current iteration is first occurence of new char
              prevChar = currentChar;
              compData->n++;
          }
      }
  }
  if(prevCharCount != 0) {
      compData->value[compData->n] = prevChar;
      compData->count[compData->n] = prevCharCount;
      compData->n++;
  }
  return (void*)compData;
}
 
 
 
int main(int argc,char *argv[]){
 
 
  // dividing the file among threads
  // make sure to make each chunk of the file and
  // assign to each thread
  // 4 core machines , it seems on best linux
  int num_threads = get_nprocs();
  //printf(" No. of threads available are %d\n",num_threads);
  
  
   int i;
   int exist;
   int fptr[argc];
   struct stat finfo[argc];
   int status[argc];
   char* mapinit;
 
 if (argc <2)
 {
     printf("pzip: file1 [file2 ...]\n");
     return 1;
 }
   
   struct chunk chunkserver[argc][num_threads];
   int file_skip[argc];

   for(i =1; i < argc; i++)
   {
          // read the file
       exist = fileExists((char *)argv[i]);
       if(exist == 0){
       // make the file
       //printf(" file doesn't exist \n");
            file_skip[i] = 1;
           continue;
       }
 
       //printf(" file exist \n");
       fptr[i] = open((char *)argv[i],O_RDONLY);
 
       // use fstat for file size in bytes
       status[i] = fstat(fptr[i],&finfo[i]);
       if(status[i] != 0 || finfo[i].st_size == 0){
           //printf(" incorrect file size or fstat failed\n");
           close(fptr[i]);
           return 0;
       }
      
       // use mmap as instructed in readme of P3A
       // adrss == NULL, let the kernel asdadecide the starting address
       // offset == 0 , start at the file entry
       mapinit = mmap(NULL,finfo[i].st_size,PROT_READ,MAP_SHARED,fptr[i],0);
       if(mapinit == MAP_FAILED){
           //printf("MMAP failed \n");
           close(fptr[i]);
           return 0;
       }
       //printf("MMAP created  \n");
 
       
       // some variable
       int chunk_start = 0;
       int chunk_len = finfo[i].st_size/num_threads;
       int chunk_rem = 0;
       if( finfo[i].st_size%num_threads != 0)
          chunk_rem = finfo[i].st_size%num_threads;   // last chunk
       //printf("File size is %ld Chunk lenth shold be %d %d\n",finfo.st_size,chunk_len,chunk_rem);
 
 
       // define all the threads
       //pthread_t th_compress[num_threads];
       // define metadata for each chunk
       //struct chunk chunkserver[num_threads];
       int start = 0;
       int end = 0;
       for(int j = 0;j<num_threads;j++){
           if(j == num_threads-1 && chunk_rem != 0){
           // a little change for last thread to
           // contain for all data
           start = chunk_start;
           end = finfo[i].st_size;
           }
           else{
               start = chunk_start;
               end = start + chunk_len - 1; // inclusive
               chunk_start = end + 1;
           }
           chunkserver[i][j].start = start;
           chunkserver[i][j].end = end;
           chunkserver[i][j].ptr = mapinit;
       }
   }

   //if(argc ==2 && (file_skip!=0))
   //{
   //    return 0;
   //}

    int flag_noValidFile =1;
   for (int i =1; i<argc; i++)
   {
       if(file_skip[i] != 1)
       {
           flag_noValidFile=0;
           break;
       }
   }
   
   if(flag_noValidFile ==1)
   {
       return 0;
   }
   //starting individual threads
   struct chunk args[argc][num_threads];
   pthread_t zip_threads[argc][num_threads];
   struct ret_Chunk* retArr[argc][num_threads];
 
  for(i =1; i < argc; i++)
  {
      if(file_skip[i] ==1)
      {
          continue;
      }
      for(int j = 0;j<num_threads;j++){
 
      args[i][j] = (struct chunk) {chunkserver[i][j].ptr, chunkserver[i][j].start, chunkserver[i][j].end};
      //printf(" Chunk data is %d %d\n",chunkserver[i][j].start,chunkserver[i][j].end);
     
      //for debugging compress
      //retArr[i][j] = (struct ret_Chunk*) compress(&args[i][j]);
 
      pthread_create(&zip_threads[i][j], NULL, compress, &args[i][j]);
 
      }
  }
 
 
  //take the return struct from the threads
   for(i =1; i < argc; i++)
   {
      if(file_skip[i] ==1)
      {
          continue;
      }
       for(int j = 0;j<num_threads;j++){
           pthread_join(zip_threads[i][j], (void*)(&retArr[i][j]));
       }
   }   
 
   //debugging the return arrays
   for(i =1; i < argc; i++)
   {
      if(file_skip[i] ==1)
      {
          continue;
      }

       for(int j = 0;j<num_threads;j++){
           for(int k = 0;k<retArr[i][j]->n;k++){
               //printf(" ret data is %c %d\n",retArr[i][j]->value[k],retArr[i][j]->count[k]);
           }
           //printf("\n");
       }
   }
 
  
 
   // joining boundary data of chunks
   char prevValue = retArr[1][0]->value[0];
   int prevCount = retArr[1][0]->count[0];
   int firstFlag =0;
 
   for(i =1; i < argc; i++)
   {  
      if(file_skip[i] ==1)
      {
          continue;
      }
       for(int j = 0;j<num_threads;j++)
       {
           for(int k = 0;k<retArr[i][j]->n;k++)
           {
               if(firstFlag==0)
               {
                   firstFlag =1;
                   continue;
               }
 
 
               if(prevValue == retArr[i][j]->value[k])
               {
                   prevCount += retArr[i][j]->count[k];
               }
               else
               {
                   //printf(" final 1 merged ret data is %c %d\n",prevValue,prevCount);
                   fwrite(&prevCount,sizeof(int),1,stdout);
                   fwrite(&prevValue,sizeof(char),1,stdout);
                   prevValue = retArr[i][j]->value[k];
                   prevCount = retArr[i][j]->count[k];
 
               }
           }
           //printf("\n");
       }
       //printf(" final 2 merged ret data is %c %d\n",prevValue,prevCount);
   }
   //printf(" final 2 merged ret data is %c %d\n",prevValue,prevCount);
   fwrite(&prevCount,sizeof(int),1,stdout);
   fwrite(&prevValue,sizeof(char),1,stdout);
  
  return 0;
}
 
