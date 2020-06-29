#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <time.h>

#define BUFFER_SIZE 4000

void substring(char [], char[], int, int);
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
       /* printf("Retrieved line of length %zu :\n", read1);
        printf("%s", line);*/
    }
	vectorfile = fopen(argv[2], "r");
	int vector_index = 0;
	int vector_array[vectorLines];
	while((read1 = getline(&line, &len, vectorfile)) != -1){
		int a = 0;
		while( a < len){
			vector_index = atoi(line) - 1;
			if(	line[a] == ' '){
				int vector_value = atoi(line + a);	
				vector_array[vector_index] = vector_value;
						
				//printf("vector value is %d \n",vector_value);			
			}
			a++;
		}			
	}
	fflush(vectorfile);
	free(line);
	//free(line);
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

    //GENERATING K PIPES REDUCE PIPE SONRA YAP [i] ile ayır her pipe bi dosya gibi tanınıyo
    int mypipe[K][2];
    for(int i = 0; i < K; i++){
        pipe(mypipe[i]);
    }

    tempK = K;
	pid_t n;
	int textFileNo = 1;
    int pipe_no = 0;

	while(tempK > 0){
		
        FILE*split_child;
		FILE *vector;
		
		n = fork();
		
		if(n < 0){
			fprintf(stderr, "Fork Failed");
			exit(-1);
		}
		//cCHILD process starts
		else if(n == 0){
			//printf("I am a child and my PID is %d my textfile is split%d.\n", getpid(),textFileNo);
			char buf[1000];
			//int vector_array[vectorLines];
			int result_array[vectorLines];//result arraye koyuyorum result arrayden file a yazıyorum
			
			//create zero array and initialize all members to zero			
			for(int i = 0; i <vectorLines; i++){
				result_array[i] = 0;
			}
			
			
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


			
            //close unused pipes
            for(int i = 0; i < K; i++){
                for(int j = 0; j < 2; j++){
                    if(i != pipe_no ){
                        close(mypipe[i][j]);
                    }
                    if(i == pipe_no){
                        close(mypipe[i][0]);
                    }
                }
            }
	

			// getline(&lineSplit, &len, split_child);
			//printf("LINE IS %s\n",lineSplit);
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
				//printf("OLD ROW %d\n", result_array[row-1]);
				result_array[row - 1] = result_array[row - 1] +(value* vector_array[column-1]); 
				//printf("CAL ROW %d COL %d VECTOR %d VALUE %d SONUC %d\n",row,column,vector_array[column-1],value, result_array[row-1]);	
				
	            /*printf("We'll multiply %d with %d and put it in %dth position\n", value,vector_array[column-1] , row-1);
				printf("The result: %d\n",result_array[row - 1] );*/
			}

			fflush(split_child);
			char pipe_line[1000] = {};
			for(int i = 0; i < vectorLines; i++){
				//BURAYI DÜZELT İTS NOT GONNA WORK
				//printf("process %d result arr %d\n", getpid(), result_array[i]);
				if(result_array[i] != 0){      
                    sprintf(pipe_line + strlen(pipe_line), "%d %d\n", i + 1, result_array[i]);
                    
				}
			}
            write(mypipe[pipe_no][1], pipe_line, strlen(pipe_line) + 1);
            //printf("WRITE %s\n",pipe_line );
            close(mypipe[pipe_no][1]);
			exit(0);	
		}
		pipe_no++;
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
			
			int pipe_number = 0;
			
            int row_number;
			int pipe_value;
			
			//opening inter filesPIPE BURDA KALDIM
			while(pipe_number < K){
                char inter_buffer[4000];
                char sub_buffer[4000];
				close(mypipe[pipe_number][1]);
                read(mypipe[pipe_number][0],inter_buffer,BUFFER_SIZE);
				//printf("inter buffer %s\n",inter_buffer);
                int a = 0;
                int pos = 0;
                int length;
				while(inter_buffer[a] != '\0' ){
					if(	inter_buffer[a] == '\n'){
                        substring(inter_buffer,sub_buffer,pos, a - pos +1);
                       // printf("READ LINE %s", sub_buffer);
                        length = a - pos + 1;
						pos = a + 1;
                        
                        int c = 0;
                       
                        while(c < length){
                           
                            int row_number = atoi(sub_buffer);
                            
                            if(sub_buffer[c] == ' '){
                                int inter_value = atoi(sub_buffer + c);
                               
                                result_array[row_number - 1] = result_array[row_number - 1] +  inter_value;
                            }
                            c++;
                        }
							//vector_index++;	
							//printf("vector value is %d process is %d\n",vector_value, 					getpid());			
					}
					
                    a++;			
				}
				pipe_number++;
			
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
	printf("Total running time for MVP process is %f\n", cpu_time_used);
  	exit(0);
}
    


// C substring function definition
void substring(char s[], char sub[], int p, int l) {
    int c = 0;
    if(p == 0){
        while( c < l){
            sub[c] = s[c];
            c++;
        }
    }
    else{
        while (c < l) {
             sub[c] = s[p+c];
            c++;
         }
    }
  
   sub[c] = '\0';
}
