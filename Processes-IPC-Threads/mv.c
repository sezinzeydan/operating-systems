#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <time.h>

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
	
	/*char fileName[30] = "matrixfile";
	char txt[5] = ".txt";
	strcat(fileName,txt);*/

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
	int vectorLines = 0;
	if (vectorfile == NULL)
        exit(EXIT_FAILURE);

   while ((read1 = getline(&line, &len, vectorfile)) != -1) {
		vectorLines++;
       // printf("Retrieved line of length %zu :\n", read1);
        //printf("%s", line);
    }
	free(line);
   //exit(EXIT_SUCCESS);
	int K = atoi(argv[4]);
	//int file_descriptors[K];
	int tempK = K;
	
	char txt[5] = ".txt";
	int L = noOfLines;
	
	int inter_no = 0;
	/*Creating intermediate files*/
	while(tempK > 0){
		char intermediate_file[30] = "intermediate_file";
		char inter_file_no[10];
		inter_no++;
		char buf[1000];
		FILE *inter_file;
		sprintf(inter_file_no, "%d", inter_no);
		strcat(intermediate_file, inter_file_no);
		strcat(intermediate_file,txt);
		inter_file = fopen(intermediate_file, "a");
		tempK--;
	}
	
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
		
		//int split_des; 
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
	pid_t n;
	int textFileNo = 1;
	while(tempK > 0){
		FILE*split_child;
		FILE *vector;
		FILE *interfile;
		n = fork();
		
		if(n < 0){
			fprintf(stderr, "Fork Failed");
			exit(-1);
		}
		//child process starts
		else if(n == 0){
			//printf("I am a child and my PID is %d my textfile is split%d.\n", getpid(),textFileNo);
			char buf[1000];
			int vector_array[vectorLines];
			int result_array[vectorLines];
			
			//create zero array and initialize all members to zero			
			for(int i = 0; i <vectorLines; i++){
				result_array[i] = 0;
			}
			
			//initialize vector array members			
			int vector_index = 0;
			vector = fopen(argv[2],"r");
			if (vector == NULL)
        	exit(EXIT_FAILURE);
			while((read1 = getline(&line, &len, vector)) != -1){
				//printf("process %d line %s \n",getpid(),line);
				int a = 0;
				vector_index = atoi(line) - 1;
				while( a < len){
					if(	line[a] == ' '){
						int vector_value = atoi(line + a);	
						vector_array[vector_index] = vector_value;
						
						//printf("vector value is %d process is %d\n",vector_value, 			getpid());			
					}
					a++;
				}			
			}
			fflush(vector);
			free(line);
			
			/*for(int i = 0; i < vectorLines; i++){
				printf("%d process is %d\n",vector_array[i], getpid());
			}*/

			//start reading split files line by line and calculate products
			char filename[30] = "split";
			char split_file_no[10];
			char *lineSplit = NULL;
			sprintf(split_file_no, "%d", textFileNo);
			strcat(filename, split_file_no);	
			strcat(filename,txt);		
			split_child = fopen(filename,"r");

			//opening inter files for writing
			char intername[30] = "intermediate_file";
			char inter_file_no[10];
			sprintf(inter_file_no, "%d", textFileNo);
			strcat(intername, inter_file_no);	
			strcat(intername,txt);		
			interfile = fopen(intername,"w");
	

			// getline(&lineSplit, &len, split_child);
			

			while(getline(&lineSplit, &len, split_child) != -1){
				//printf("LINE %s process %d\n",lineSplit, getpid());	
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
							
							
							//printf("ATOI IS EVIL %s\n", lineSplit + st- 1);
						
							value = atoi(lineSplit + st );
						}
						
					}
					st++;
				}
				//printf("OLD ROW %d\n", result_array[row-1]);
				result_array[row - 1] = result_array[row - 1] +(value * vector_array[column-1]); 
				//printf("ROW %d COL%d VALUE %d VECTOR %d SONUC %d\n",row,column,value,vector_array[column-1],result_array[row-1]);	
			}

			fflush(split_child);
			
			for(int i = 0; i < vectorLines; i++){
				//char inter_line[
				//printf("process %d result arr %d\n", getpid(), result_array[i]);
				if(result_array[i] != 0){
					fprintf(interfile, "%d %d\n", i + 1, result_array[i]);			
				}
			}
			exit(0);	
		}
		
		textFileNo++;
		tempK--;
				
	}
	if(n >0){
		for(int i = 0; i < K;i++)
		wait(NULL);
		
		printf("Parent process \n");
		pid_t reducer;
		reducer = fork();
		
		//starting reducer process
		if(reducer == 0){
			printf("reducer process \n");
			int result_array[vectorLines];
			for(int i = 0; i < vectorLines; i++){
				result_array[i] = 0;
			}

			FILE* result_file = fopen(argv[3], "w");
			FILE* interfile;
			int inter_number = 1;
			int row_number;
			int inter_value;
			
			//opening inter files
			while(inter_number <= K){
				char intername[30] = "intermediate_file";
				char inter_file_no[10];
				sprintf(inter_file_no, "%d", inter_number);
				strcat(intername, inter_file_no);	
				strcat(intername,txt);		
				interfile = fopen(intername,"r");
				
				/*printf("RESULT ARRAY BEFORE ADDING IS \n");
				for(int i = 0; i < vectorLines; i++){
					printf("ROW %d VALUE %d\n",i + 1, result_array[i]);
				}*/
				
				while((read1 = getline(&line, &len, interfile)) != -1 ){
					//printf("line %s \n",line);
					int a = 0;
					while( a < len){
						int row_number = atoi(line);
						if(	line[a] == ' '){
							int inter_value = atoi(line + a);	
							result_array[row_number -1] = result_array[row_number-1] + inter_value;

							//vector_index++;	
							//printf("vector value is %d process is %d\n",vector_value, 					getpid());			
						}
						a++;
					}			
				}
				//printf("FAZLA GELIYO\n");
				fflush(interfile);
				free(line);
				inter_number++;
			
			}
			//write into result file
            /*for(int i = 0; i < vectorLines; i++){
				printf("RESULT ARR %d\n", result_array[i]);	
			}*/
			for(int i = 0; i < vectorLines; i++){
				//char inter_line[
				if(result_array[i] != 0){
					fprintf(result_file, "%d %d\n", i + 1, result_array[i]);			
				}
			}
            exit(0);
		}
		if(reducer > 0){
       		 wait(NULL);
			printf("Parent after reducer \n");
			char buf[100];
			FILE* result_file = fopen(argv[3], "r");
			while (fgets(buf,1000, result_file)!= NULL) {
        	
        		printf("%s", buf);
    		}
		
    	}
		
	}
	end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
	printf("Total running time for MV process is %f\n", cpu_time_used);
  	exit(0);
}
