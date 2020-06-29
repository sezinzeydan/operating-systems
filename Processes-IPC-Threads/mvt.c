#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <pthread.h> 

int thread_number = 0;
int vectorLines;
int result_array[10000];
int vector_array[10000];
void *myThread(void *vargp) 
{ 
    ++thread_number;
    FILE*split_child;
	FILE *vector;
    char *line = NULL;
    size_t len = 0;
    ssize_t read1;
    char txt[5] = ".txt";
    // Store the value argument passed to this thread 
    int *myid = (int *)vargp; 
  
  
    // Print the argument, static and global variable
   // printf("I am thread opening split%d.\n",thread_number);
	char buf[1000];
    //int vector_array[vectorLines];
	
			
	//create zero array and initialize all members to zero			
	for(int i = 0; i <vectorLines; i++){
		result_array[i] = 0;
	}
			
	//initialize vector array members			
	/*int vector_index = 0;
	vector = fopen("vectorfile.txt","r");
    if (vector == NULL)
        exit(EXIT_FAILURE);
	while((read1 = getline(&line, &len, vector)) != -1){
		printf("process %d line %s \n",getpid(),line);
		int a = 0;
		while( a < len){
			vector_index = atoi(line)-1;
			if(	line[a] == ' '){
            	int vector_value = atoi(line + a);	
				vector_array[vector_index] = vector_value;
				//printf("vector value is %d process is %d\n",vector_value, 			getpid());			
			}
			a++;
		}			
	}
	fflush(vector);
	free(line);*/
			
	/*for(int i = 0; i < vectorLines; i++){
		printf("%d process is %d\n",vector_array[i], getpid());
	}*/

	//start reading split files line by line and calculate products
	char filename[30] = "split";
	char split_file_no[10];
	char *lineSplit = NULL;
	sprintf(split_file_no, "%d", thread_number);
	strcat(filename, split_file_no);	
	strcat(filename,txt);		
	split_child = fopen(filename,"r");

    while(getline(&lineSplit, &len, split_child) != -1){
		//printf("line %s process %d\n",lineSplit, getpid());	
		int st = 0;
				
		int rowDone = 0;
		int colDone = 0;
		int row;
		int column;
		int value;
		int lastSpace;
		while(st < len){
				
			if(lineSplit[st] == ' '){
				if(!rowDone){			
					row = atoi(lineSplit);
					rowDone = 1;
					lastSpace = st;		
				}
				if(rowDone == 1 && colDone == 0){		
					column = atoi(lineSplit +lastSpace);//colDest);
					colDone = 1;
					lastSpace = st;
				}
				if(rowDone == 1 && colDone == 1){
					value = atoi(lineSplit + st);
				}
						
			}
			st++;				
        }
		result_array[row - 1] = result_array[row - 1] +(value* vector_array[column-1]); 
		//printf("startig calc ROW %d COL	%d VALUE %d\n",row,column,value);	
	}

	fflush(split_child);
    return NULL;
    
} 

void *reducerThread(void *vargp){
    FILE* result_file = fopen("resultfile.txt", "w");
	for(int i = 0; i < vectorLines; i++){
        //char inter_line[
		if(result_array[i] != 0){
			fprintf(result_file, "%d %d\n", i + 1, result_array[i]);			
		}
	}
    return NULL;
}


int main(int argc,char* argv[])
{
	clock_t start, end;
    double cpu_time_used;

	start = clock();
	FILE *fp;
	FILE *vectorfile;
	char *line = NULL;
    size_t len = 0;
    ssize_t read1;
	

    fp = fopen(argv[1], "r");
	vectorfile = fopen(argv[2], "r");
	int noOfLines = 0;
    if (fp == NULL)
        exit(EXIT_FAILURE);

   while ((read1 = getline(&line, &len, fp)) != -1) {
		noOfLines++;
        /*printf("Retrieved line of length %zu :\n", read1);
        printf("%s", line);*/
    }

   	free(line);
	vectorLines = 0;
	if (vectorfile == NULL)
        exit(EXIT_FAILURE);

   while ((read1 = getline(&line, &len, vectorfile)) != -1) {
		vectorLines++;
        /*printf("Retrieved line of length %zu :\n", read1);
        printf("%s", line);*/
    }
	free(line);
	int vector_index = 0;
	vectorfile = fopen("vectorfile.txt","r");
    if (vectorfile == NULL)
        exit(EXIT_FAILURE);
	while((read1 = getline(&line, &len, vectorfile)) != -1){
		//printf("process %d line %s \n",getpid(),line);
		int a = 0;
		while( a < len){
			vector_index = atoi(line)-1;
			if(	line[a] == ' '){
            	int vector_value = atoi(line + a);	
				vector_array[vector_index] = vector_value;
				//printf("vector value is %d process is %d\n",vector_value, 			getpid());			
			}
			a++;
		}			
	}
	fflush(vectorfile);
	free(line);
   //exit(EXIT_SUCCESS);
	int K = atoi(argv[4]);
	//int file_descriptors[K];
	int tempK = K;
	
	char txt[5] = ".txt";
	int L = noOfLines;
	
	
	
	int splitSize;
	int total_splited = 0;
	int splitNo=0;
	tempK = K;
	while(tempK > 0 ){
		char filename[30] = "split";
		char split_file_no[10];
		splitNo++;
		char buf[1000];
		FILE *split;
		sprintf(split_file_no, "%d", splitNo);
		strcat(filename, split_file_no);
		strcat(filename,txt);
		
		
		if(tempK == 1){
			splitSize = L - total_splited;
			
			split = fopen(filename,"w");
			
		}	
		else{
			splitSize = (L/K) + (L%K != 0);
			split = fopen(filename,"w");
		}

		
		fp = fopen(argv[1], "r");
		int range = splitSize + total_splited;
		int lineNo = 0;
		int rangeDone = 0;
		int nowCopy = 0;
		//printf( "inside loop %s split size : %d totalsplited: %d and range : 						%d\n",filename,splitSize, total_splited, range);
		while(fgets(buf,1000, fp)!=NULL && rangeDone == 0){
			//printf("nowCOPY %d ",nowCopy);
			if(range - lineNo == splitSize){
				nowCopy = 1;
			}
			lineNo++;
			if(nowCopy == 1){
				if(lineNo == 0){
					lineNo--;
				}
				fputs(buf, split);
				//printf("Put line %s :\n", buf);
				//printf("line no %d range %d \n",lineNo, range);
				if(lineNo == range){
					rangeDone = 1;
				}
			}
			
		}
		total_splited += splitSize;
		tempK--;
		fflush(split);
	}
	tempK = K;
    pthread_t tid;
    for(int i = 0; i < K; i++){
        pthread_create(&tid, NULL, myThread, NULL);
    }	
    for(int i = 0; i < K; i++){
        pthread_join(tid, NULL); 
    }

    
	printf("Parent process \n");
	pthread_t reducer;
	pthread_create(&tid, NULL, reducerThread, NULL);
	pthread_join(reducerThread, NULL);	
    
	printf("Parent after reducer \n");
	char buf[1000];
	FILE* result_file = fopen(argv[3], "r");
	while (fgets(buf,1000, result_file)!= NULL) {
        printf("%s", buf);
    }
	end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
	printf("Total running time for MVT is %f\n", cpu_time_used);
	exit(0);
 }
		
	
