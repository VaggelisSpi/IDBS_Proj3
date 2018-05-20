#include "sort_file.h"
#include "bf.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "Inplace_Sorting.h"
#include "Merge_Sorting.h"

#define CALL_OR_ERROR(call)     \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK) {      \
			return SR_ERROR;           \
		}                         \
	}

#define CALL_OR_ERROR2(call)     \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK) {      \
			BF_Block_Destroy(&block); \
			return SR_ERROR;           \
		}                         \
	}

#define CALL_OR_ERROR3(call)     \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK) {      \
			BF_Block_Destroy(&inputBlock); \
			BF_Block_Destroy(&tempBlock); \
			return SR_ERROR;           \
		}                         \
	}

SR_ErrorCode SR_Init() {
	/* We have no structs to initialize so we just return SR_OK .*/
	return SR_OK;
}

SR_ErrorCode SR_CreateFile(const char *fileName) {

	BF_Block *block;
	int fd,i,x,offs;
	char* data;

	CALL_OR_ERROR(BF_CreateFile(fileName));
	CALL_OR_ERROR(BF_OpenFile(fileName,&fd));

	BF_Block_Init(&block);
	CALL_OR_ERROR2(BF_AllocateBlock(fd,block));

	data = BF_Block_GetData(block);

	offs=0;

	x=999;
	memcpy(data,&x,sizeof(int));  /* Store number 999 .*/
	offs += sizeof(int);

	x=-999;
	memcpy(data+offs,&x,sizeof(int)); /* Store number -999 .*/
	offs += sizeof(int);

	x=0; /* counter */
	memcpy(data+offs,&x,sizeof(int)); /* Store counter of records which is 0 at the beggining .*/
	offs += sizeof(int);

	memset(data+offs,1,BF_BLOCK_SIZE-offs); /* Fill the remaining block with 1 .*/

	BF_Block_SetDirty(block);
	CALL_OR_ERROR2(BF_UnpinBlock(block));
	BF_CloseFile(fd);
	BF_Block_Destroy(&block);

	return SR_OK;

}

SR_ErrorCode SR_OpenFile(const char *fileName, int *fileDesc) {

	BF_Block *block;
	char* data;
	int x,offs=0;
	char y;

	CALL_OR_ERROR(BF_OpenFile(fileName,fileDesc));
	BF_Block_Init(&block);
	CALL_OR_ERROR2(BF_GetBlock(*fileDesc,0,block));

	data = BF_Block_GetData(block);

	memcpy(&x,data,sizeof(int));
	if(x!=999){		/* First int must be the number 999 else it is not an SR_FILE .*/
		CALL_OR_ERROR2(BF_UnpinBlock(block));
		BF_Block_Destroy(&block);
		return SR_ERROR;
	}
	offs+=sizeof(int);

	memcpy(&x,data+offs,sizeof(int));
	if(x!=-999){	/* Second int must be the number -999 else it is not an SR_FILE .*/
		CALL_OR_ERROR2(BF_UnpinBlock(block));
		BF_Block_Destroy(&block);
		return SR_ERROR;
	}
	offs+=sizeof(int);

	memcpy(&x,data+offs,sizeof(int));
	if(x<0){	/* Third int ,which is the counter of records, must be a number >= 0 else it is not an SR_FILE .*/
		CALL_OR_ERROR2(BF_UnpinBlock(block));
		BF_Block_Destroy(&block);
		return SR_ERROR;
	}
	offs+=sizeof(int);


	while( offs < BF_BLOCK_SIZE ){

		memcpy(&y,data+offs,sizeof(char));
		if(y!=1){
			CALL_OR_ERROR2(BF_UnpinBlock(block));
			BF_Block_Destroy(&block);
			return SR_ERROR;
		}
		offs+=sizeof(char);

	}

	CALL_OR_ERROR2(BF_UnpinBlock(block));
	BF_Block_Destroy(&block);

	return SR_OK;
}

SR_ErrorCode SR_CloseFile(int fileDesc) {
	CALL_OR_ERROR(BF_CloseFile(fileDesc));
	return SR_OK;
}

SR_ErrorCode SR_InsertEntry(int fileDesc,  Record record) {

	int blocks_num,new_block=0,counter,offs=0,stop;
	BF_Block *block;
	char* data;

	BF_Block_Init(&block);

	CALL_OR_ERROR2(BF_GetBlockCounter(fileDesc,&blocks_num));
	if( blocks_num == 1 ){ /* We have only the first block with the specific info .*/
		new_block=1;
	}
	else{ /* We have at least one block except the first one with the specific info .*/

		CALL_OR_ERROR2(BF_GetBlock(fileDesc,blocks_num-1,block)); /* Τake the last block .*/
		data = BF_Block_GetData(block);

		memcpy(&counter,data,sizeof(int)); /* Ηow many records there are in this block .*/

		if( counter < MAX_RECORDS ){	/* Check if there is free space to store the record .*/
			offs = ( counter * RECORD_SIZE ) + sizeof(int);
			counter++;
			memcpy(data,&counter,sizeof(int)); //update counter
		}
		else{ 	/* There is no free space .*/
			CALL_OR_ERROR2(BF_UnpinBlock(block)); /* We didnt change this block so we don't need to set it dirty .*/
			new_block = 1;
		}
	}

	if( new_block==1 ){ /* We need new block .*/

		counter=1;
		CALL_OR_ERROR2(BF_AllocateBlock(fileDesc,block));
		data = BF_Block_GetData(block);
		memcpy(data,&counter,sizeof(int)); /* Save the number of records in this block,it is 0 at the beggining and now is 1 as we will insert a new record */
		offs += sizeof(int);

	}

	/* Store record .*/
	memcpy(data+offs,&(record.id),sizeof(int));
	offs += sizeof(int);

	memcpy(data+offs,record.name,strlen(record.name)+1);
	stop = sizeof(record.name)-(strlen(record.name)+1);
	memset(data+offs+strlen(record.name)+1,0,stop); /* Fill the unitialized bytes from strlen+1 till sizeof(record.name) with 0 .*/
	offs += sizeof(record.name);

	memcpy(data+offs,record.surname,strlen(record.surname)+1);
	stop = sizeof(record.surname)-(strlen(record.surname)+1);
	memset(data+offs+strlen(record.surname)+1,0,stop);
	offs += sizeof(record.surname);

	memcpy(data+offs,record.city,strlen(record.city)+1);
	stop = sizeof(record.city)-(strlen(record.city)+1);
	memset(data+offs+strlen(record.city)+1,0,stop);
	offs += sizeof(record.city);

	BF_Block_SetDirty(block);
	CALL_OR_ERROR2(BF_UnpinBlock(block));

	/* Update total records counter at the first block of the file .*/
	CALL_OR_ERROR2(BF_GetBlock(fileDesc,0,block));

	data = BF_Block_GetData(block);

	memcpy(&counter,data+2*sizeof(int),sizeof(int));
	counter++;

	memcpy(data+2*sizeof(int),&counter,sizeof(int));

	BF_Block_SetDirty(block);
	CALL_OR_ERROR2(BF_UnpinBlock(block));

	BF_Block_Destroy(&block);

	return SR_OK;

}

SR_ErrorCode SR_SortedFile(
	const char* input_filename,
	const char* output_filename,
	int fieldNo,
	int bufferSize) {

	int tempFileDesc, inputFileDesc,i,inputBlocks;
	char *inputData,*tempData;
	BF_Block *inputBlock,*tempBlock;

	if( (bufferSize < 3) || (bufferSize > BF_BUFFER_SIZE) ){
		printf("Invalid bufferSize!\n");
		return SR_ERROR;
	}

	BF_CreateFile("temp_file1.tmp");
	BF_OpenFile("temp_file1.tmp", &tempFileDesc);

	BF_OpenFile(input_filename, &inputFileDesc);

	CALL_OR_ERROR(BF_GetBlockCounter(inputFileDesc,&inputBlocks));

	BF_Block_Init(&inputBlock);
	BF_Block_Init(&tempBlock);

	/* Copy content from inputFile to tempFile */
	for( i=0; i<inputBlocks; i++ ){

		CALL_OR_ERROR3(BF_GetBlock(inputFileDesc,i,inputBlock));
		CALL_OR_ERROR3(BF_AllocateBlock(tempFileDesc,tempBlock));

		inputData = BF_Block_GetData(inputBlock);
		tempData = BF_Block_GetData(tempBlock);

		memcpy(tempData,inputData,BF_BLOCK_SIZE);

		CALL_OR_ERROR3(BF_UnpinBlock(inputBlock));
		BF_Block_SetDirty(tempBlock);
		CALL_OR_ERROR3(BF_UnpinBlock(tempBlock));

	}

	BF_Block_Destroy(&inputBlock);
	BF_Block_Destroy(&tempBlock);

	BF_CloseFile(inputFileDesc);

	if( inPlace_Sorting(tempFileDesc, bufferSize, fieldNo) != 1 ){
		return SR_ERROR;
	}

	if( mergeSort(tempFileDesc,output_filename,bufferSize,fieldNo) != 1 ){
		return SR_ERROR;
	}

	return SR_OK;
}

SR_ErrorCode SR_PrintAllEntries(int fileDesc) {

	int blocks_num,offs,i,j,counter;
	BF_Block *block;
	char* data;
	Record record;

	BF_Block_Init(&block);
	CALL_OR_ERROR2(BF_GetBlockCounter(fileDesc,&blocks_num));

	for(i=1;i<blocks_num;i++){ //skip first block with the specific info
		CALL_OR_ERROR2(BF_GetBlock(fileDesc,i,block));
		data = BF_Block_GetData(block);
		memcpy(&counter,data,sizeof(int));

		offs = sizeof(int);
		for(j=0;j<counter;j++){

			memcpy(&(record.id),data+offs,sizeof(record.id));
			offs += sizeof(record.id);

			memcpy(record.name,data+offs,sizeof(record.name));
			offs += sizeof(record.name);

			memcpy(record.surname,data+offs,sizeof(record.surname));
			offs += sizeof(record.surname);

			memcpy(record.city,data+offs,sizeof(record.city));
			offs += sizeof(record.city);

			printf("%d,\"%s\",\"%s\",\"%s\"\n",\
			record.id, record.name, record.surname, record.city);

		}
		CALL_OR_ERROR2(BF_UnpinBlock(block));
	}

	BF_Block_Destroy(&block);

	return SR_OK;

}
