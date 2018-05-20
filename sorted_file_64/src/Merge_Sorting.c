#include "sort_file.h"
#include "bf.h"
#include "Merge_Sorting.h"
#include "Inplace_Sorting.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#define CALL_OR_ERROR(call)     \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK) {      \
			BF_PrintError(code);\
			return -1;           \
		}                         \
	}

#define CALL_OR_ERROR2(call)     \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK) {      \
			BF_Block_Destroy(&block); \
			BF_PrintError(code);\
			return -1;           \
		}                         \
	}

#define CALL_OR_ERROR3(call)     \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK) {      \
			BF_Block_Destroy(&block); \
			BF_Block_Destroy(&outBlock); \
			BF_PrintError(code);\
			return -1;           \
		}                         \
	}

#define CALL_OR_ERROR4(call)     \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK) {      \
			free(groupsArray); \
			BF_Block_Destroy(&(outDataBlock.block)); \
			BF_PrintError(code);\
			return -1;           \
		}                         \
	}

#define CALL_OR_ERROR5(call)     \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK) {      \
			free(groupsArray); \
			BF_Block_Destroy(&(outDataBlock.block)); \
			for( i=0; i<thisMerge.numberOfGroups; i++ ){ \
				if( inDataBlocks[i].data != NULL ) {	 \
					BF_UnpinBlock(inDataBlocks[i].block);\
				}										 \
			}											 \
			free(inDataBlocks);							 \
			BF_PrintError(code);						 \
			return -1;           					 \
		}                         						 \
	}

#define CALL_OR_ERROR6(call)     \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK) {      \
			BF_Block_Destroy(&block1); \
			BF_Block_Destroy(&block2); \
			BF_PrintError(code);\
			return -1;           \
		}                         \
}

#define CALL_OR_ERROR7(call)     \
	{                           \
		int code = call; \
		if (code == -1) {      \
			BF_Block_Destroy(&block); \
			BF_Block_Destroy(&outBlock); \
			BF_PrintError(code);\
			return -1;           \
		}                         \
	}

#define CALL_OR_ERROR8(call)     \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK) {      \
			BF_PrintError(code);\
			return code;           \
		}                         \
	}

/* Implementing merge sorting at groups we have quick sorted before with Inplace_Sorting */
int mergeSort(int inputFileDesc, const char *output_filename,int bufferSize, int fieldNo){

	int totalBlocksNumber,i,lastTimeGroups,times,nextGroups,totalNumberOfGroups,lastBlockRecordsOfLastGroup;
	int tempInputDesc,tempOutputDesc,tempFileDesc,outputFileDesc,tempOutputNumber;
	BF_Block *block,*outBlock;
	char *data,*outData;
	char tempOutputName[15];
	mergeInfo currentMerge;
	FILE *temp;

	CALL_OR_ERROR(BF_GetBlockCounter(inputFileDesc,&totalBlocksNumber));
	totalBlocksNumber--;

	/* Number of records in last block will be always the same so we can take this info here */
	/* Last block is the only block which may be not full so we need to know the number of records in it */
	BF_Block_Init(&block);
	CALL_OR_ERROR2(BF_GetBlock(inputFileDesc,totalBlocksNumber,block));
	data = BF_Block_GetData(block);
	memcpy( &(lastBlockRecordsOfLastGroup), data , sizeof(int) );
	CALL_OR_ERROR2(BF_UnpinBlock(block));

	totalNumberOfGroups = ( totalBlocksNumber / bufferSize ) + ( (totalBlocksNumber % bufferSize) != 0 ); /* (totalBlocksNumber % bufferSize) != 0 means
																										that there is an extra group ( without max number of blocks ) which
																										has not been calculated in division ( totalBlocksNumber / bufferSize )  */
	currentMerge.blocksInEachGroup = bufferSize; /* But if totalBlocksNumber < bufferSize we will have only one group that may have blockNumber < bufferSize but we have no problem with that as we wont execute the while statement */

	/* First time the inputFileDesc will be the tempInputDesc and the new tempFile will be the tempOutputDesc */
	tempInputDesc = inputFileDesc;
	BF_CreateFile("temp_file2.tmp");
	BF_OpenFile("temp_file2.tmp", &tempOutputDesc);
	tempOutputNumber = 1;

	/*Copy first block from tempInputFile to tempOutputFile */
	CALL_OR_ERROR7(copyAtEnd(tempInputDesc,tempOutputDesc,0,0));

	BF_Block_Init(&outBlock);

	while( totalNumberOfGroups != 1 ){

		/* Calculate how many times the merge ( for loop ) should be done */
		times = totalNumberOfGroups / ( bufferSize -1 );
		nextGroups = times +1; /* numberOfGroups for the next while loop */

		if( totalNumberOfGroups % ( bufferSize - 1 ) == 0 ){ /* It means that all groups have the max number of blocks in a group  */
			/* So we need to times-- as we do special sort for the last time groups */
			times --;
			nextGroups --;
		} /* Otherwise it means that there are groups which do (bufferSize-1)-pair with other groups but there are extra groups
			which they dont (bufferSize-1)-pair but maybe < (bufferSize-1) or they are alone, so we don't need to do times-- */

		/* Merge groups taking them per bufferSize-1 groups */
		currentMerge.lastBlockRecords = MAX_RECORDS;
		for( i=0; i<times; i++ ){

			/* In this for loop we have always : */
			/* # (bufferSize-1) groups to merge */
			/* # each group has blocksInEachGroup blocks */
			/* # all blocks in all groups are full, that means they have MAX_RECORDS records */
			currentMerge.beginBlock = i * ( currentMerge.blocksInEachGroup * (bufferSize-1) ) + 1 ; /* +1 as we dont begin from 0 but from 1, as first block has special information */
			currentMerge.lastGroupBlocks = currentMerge.blocksInEachGroup;
			currentMerge.numberOfGroups = (bufferSize-1);
			mergeGroups(tempInputDesc,tempOutputDesc,currentMerge,bufferSize,fieldNo);

		}

		/* We need to do special merge at the last time */
		lastTimeGroups = totalNumberOfGroups % ( bufferSize - 1 ); /* Number of groups for the last merge */
		currentMerge.lastGroupBlocks = totalBlocksNumber - (( totalNumberOfGroups - 1 ) * currentMerge.blocksInEachGroup);
		currentMerge.beginBlock = i * ( currentMerge.blocksInEachGroup * (bufferSize-1) ) + 1 ;
		currentMerge.lastBlockRecords = lastBlockRecordsOfLastGroup;

		if( lastTimeGroups == 1 ){ /* We just write this group as it is, without doing any merge as there is no other group at this time */

			/* Copy this group as it is in the end of the tempOutputDesc */
			CALL_OR_ERROR7(copyAtEnd(tempInputDesc,tempOutputDesc,currentMerge.beginBlock,(currentMerge.beginBlock + currentMerge.lastGroupBlocks)-1));

		}
		else{
			/* Here it is not sure that we have always the specifications we had at the for loop above */
			/* # we may have less than (bufferSize-1) groups */
			/* # last group may not have blocksInEachGroup but it may have less blocks */
			/* # last block in last group may not have MAX_RECORDS but it may have less records, it has lastBlockRecords */

			if( lastTimeGroups == 0 ){ /* Last time groups are bufferSize-1 */
				lastTimeGroups = bufferSize-1;
			}

			currentMerge.numberOfGroups = lastTimeGroups;
			mergeGroups(tempInputDesc,tempOutputDesc,currentMerge,bufferSize,fieldNo);
		}

		/* Preparation for the next loop */
		totalNumberOfGroups = nextGroups;
		currentMerge.blocksInEachGroup *= (bufferSize-1);

		/* Swap current tempInputDesc with current tempOutputDesc */
		tempFileDesc = tempInputDesc;
		tempInputDesc = tempOutputDesc;
		tempOutputDesc = tempFileDesc;
		tempOutputNumber = ( tempOutputNumber != 1 ); /* swap 0 -> 1 or 1 -> 0 */
		sprintf(tempOutputName,"temp_file%d.tmp",tempOutputNumber+1);
		/* And clear tempOutputDesc */
		BF_CloseFile(tempOutputDesc);
		temp = fopen(tempOutputName,"w"); /* Clear the file */
		fclose(temp);
		BF_OpenFile(tempOutputName, &tempOutputDesc);
		/*Copy first block from tempInputFile to tempOutputFile */
		CALL_OR_ERROR7(copyAtEnd(tempInputDesc,tempOutputDesc,0,0));

	}

	BF_CloseFile(tempOutputDesc);

	BF_Block_Destroy(&block);
	BF_Block_Destroy(&outBlock);

	/*Copy last tempInputDesc to output_filename */
	BF_CreateFile(output_filename);
	BF_OpenFile(output_filename, &outputFileDesc);

	copyAtEnd(tempInputDesc,outputFileDesc,0,totalBlocksNumber);

	BF_CloseFile(tempInputDesc);

	/* Destroy temp files */
	if( remove("temp_file1.tmp") != 0 ){
		return -1;
	}

	if( remove("temp_file2.tmp") != 0 ){
		return -1;
	}

	BF_CloseFile(outputFileDesc);
	return 1;

}

int mergeGroups(int inputFileDesc,int outputFileDesc,mergeInfo thisMerge,int bufferSize,int fieldNo){

	int i,blockNum,firstGroup,lastGroup,outBlockFullness,minGroup,x= MAX_RECORDS,outDataOffset,recordsSpace,remainingRecords,lastBlock;
	groupInfo *groupsArray; /* Information about each group that is in this merge */
	dataBlock *inDataBlocks,outDataBlock;

	groupsArray = malloc( thisMerge.numberOfGroups * sizeof(groupInfo) );

	/* Initialization of the array. */
	blockNum = thisMerge.beginBlock;
	for( i=0; i<thisMerge.numberOfGroups-1; i++ ){

		groupsArray[i].currentBlock = blockNum;
		groupsArray[i].currentOffset = sizeof(int);
		groupsArray[i].maxBlock = blockNum + ( thisMerge.blocksInEachGroup - 1 );

		blockNum += thisMerge.blocksInEachGroup;

	}
	/* Special initialization for the last group. */
	groupsArray[i].currentBlock = blockNum;
	groupsArray[i].currentOffset = sizeof(int);
	groupsArray[i].maxBlock = blockNum + ( thisMerge.lastGroupBlocks - 1 );

	/* Initialization of dataBlocks */
	BF_Block_Init(&(outDataBlock.block));
	CALL_OR_ERROR4(BF_AllocateBlock(outputFileDesc,outDataBlock.block));
	outDataBlock.data = BF_Block_GetData(outDataBlock.block);
	/* For all blocks except last, counter will be always MAX_RECORDS */
	memcpy(outDataBlock.data,&x,sizeof(int));

	inDataBlocks = malloc( thisMerge.numberOfGroups * sizeof(dataBlock) );

	/* First we initialize with NULL so as to know what to unpin if it needs */
	for( i=0; i<thisMerge.numberOfGroups; i++ ){
		inDataBlocks[i].data = NULL;
	}

	lastBlock = (thisMerge.beginBlock + ( (thisMerge.numberOfGroups-1)  * thisMerge.blocksInEachGroup) - 1 + thisMerge.lastGroupBlocks );

	for( i=0; i<thisMerge.numberOfGroups; i++ ){
		BF_Block_Init(&inDataBlocks[i].block);
		CALL_OR_ERROR5(BF_GetBlock(inputFileDesc,groupsArray[i].currentBlock,inDataBlocks[i].block));
		inDataBlocks[i].data = BF_Block_GetData(inDataBlocks[i].block);
		CALL_OR_ERROR5(getMergeRecord(inputFileDesc,&(inDataBlocks[i]),&(groupsArray[i]),thisMerge.lastBlockRecords,lastBlock));
	}

	/* At the beggining firstGroup is 0 and lastGroup is thisMerge.numberOfGroups-1 */
	firstGroup = 0;
	lastGroup = thisMerge.numberOfGroups-1;
	outBlockFullness = 0;
	outDataOffset = sizeof(int);

	while( firstGroup != lastGroup ){

		/* Find record with smallest fieldNo of all groups' currentBlock */
		minGroup = firstGroup;
		outDataBlock.record = inDataBlocks[minGroup].record;

		for( i=firstGroup+1; i<=lastGroup; i++ ){

			if( groupsArray[i].currentOffset != -1 ){
				if( compare(inDataBlocks[i].record,outDataBlock.record,fieldNo) ){ /* Means that inDataBlocks[i].record <= outDataBlock.record  */
					minGroup = i;
					outDataBlock.record = inDataBlocks[minGroup].record;
				}
			} /* Else skip this group as it has nothing more */

		}

		/* Write minRecord to outDataBlock and check if we need to do firstGroup++ or lastGroup-- */
		memcpy(outDataBlock.data + outDataOffset,&(outDataBlock.record.id),sizeof(outDataBlock.record.id));
		outDataOffset += sizeof(outDataBlock.record.id);

		memcpy(outDataBlock.data + outDataOffset,outDataBlock.record.name,sizeof(outDataBlock.record.name));
		outDataOffset += sizeof(outDataBlock.record.name);

		memcpy(outDataBlock.data + outDataOffset,outDataBlock.record.surname,sizeof(outDataBlock.record.surname));
		outDataOffset += sizeof(outDataBlock.record.surname);

		memcpy(outDataBlock.data + outDataOffset,outDataBlock.record.city,sizeof(outDataBlock.record.city));
		outDataOffset += sizeof(outDataBlock.record.city);

		outBlockFullness ++;

		if( outBlockFullness == MAX_RECORDS ){
			BF_Block_SetDirty(outDataBlock.block);
			CALL_OR_ERROR5(BF_UnpinBlock(outDataBlock.block));
			CALL_OR_ERROR5(BF_AllocateBlock(outputFileDesc,outDataBlock.block));
			outDataBlock.data = BF_Block_GetData(outDataBlock.block);
			/* For all blocks except last, counter will be always MAX_RECORDS */
			memcpy(outDataBlock.data,&x,sizeof(int));
			outBlockFullness = 0;
			outDataOffset = sizeof(int);
		}

		if( groupsArray[minGroup].currentBlock == -1 ){
			groupsArray[minGroup].currentOffset = -1; /* currentOffset == -1 means that this block is off */
			if( firstGroup == minGroup ){
				firstGroup ++;
				while( groupsArray[firstGroup].currentOffset == -1 ){ /* As firstGroup++ may be off */
					firstGroup ++;
				}
			}
			else if( lastGroup == minGroup ){
				lastGroup --;
				while( groupsArray[lastGroup].currentOffset == -1 ){ /* As lastGroup-- may be off */
					lastGroup --;
				}
			}
		}
		else{
			CALL_OR_ERROR5(getMergeRecord(inputFileDesc,&(inDataBlocks[minGroup]),&(groupsArray[minGroup]),thisMerge.lastBlockRecords,lastBlock));
		}

	}

	/* firstGroup == lastGroup that means now we have only one group left */

	/* So we just write its remaining blocks/records to the outDataBlock/outputFileDesc */
	if( groupsArray[firstGroup].currentBlock == -1 ){ /* We only have to write inDataBlocks[firstGroup].record */

		outDataBlock.record = inDataBlocks[firstGroup].record;

		memcpy(outDataBlock.data + outDataOffset,&(outDataBlock.record.id),sizeof(outDataBlock.record.id));
		outDataOffset += sizeof(outDataBlock.record.id);

		memcpy(outDataBlock.data + outDataOffset,outDataBlock.record.name,sizeof(outDataBlock.record.name));
		outDataOffset += sizeof(outDataBlock.record.name);

		memcpy(outDataBlock.data + outDataOffset,outDataBlock.record.surname,sizeof(outDataBlock.record.surname));
		outDataOffset += sizeof(outDataBlock.record.surname);

		memcpy(outDataBlock.data + outDataOffset,outDataBlock.record.city,sizeof(outDataBlock.record.city));
		outDataOffset += sizeof(outDataBlock.record.city);

		outBlockFullness ++;

		/* Write correct counter of the last block */
		memcpy(outDataBlock.data,&(thisMerge.lastBlockRecords),sizeof(int));
	}
	else{

		if( groupsArray[firstGroup].currentOffset == sizeof(int) ){ /* We should go to the last record of previous block */
			CALL_OR_ERROR5(BF_UnpinBlock(inDataBlocks[firstGroup].block));
			groupsArray[firstGroup].currentBlock -- ;
			CALL_OR_ERROR5(BF_GetBlock(inputFileDesc,groupsArray[firstGroup].currentBlock,inDataBlocks[firstGroup].block));
			inDataBlocks[firstGroup].data = BF_Block_GetData(inDataBlocks[firstGroup].block);
			groupsArray[firstGroup].currentOffset = sizeof(int) + (MAX_RECORDS-1) * RECORD_SIZE;
		}
		else{
			groupsArray[firstGroup].currentOffset -= RECORD_SIZE; /* currentOffset was at the next record but we need to know the offset of current record */
		}

		while( groupsArray[firstGroup].currentBlock < groupsArray[firstGroup].maxBlock ){

			remainingRecords = ( (MAX_RECORDS * RECORD_SIZE) - ( groupsArray[firstGroup].currentOffset - sizeof(int) ) ) / RECORD_SIZE; /* remaining records to copy from currentBlock */

			recordsSpace = MAX_RECORDS - outBlockFullness; /* how many records we can write in the outDataBlock before we allocate new block */

			if( recordsSpace >= remainingRecords ){ /* There is space for remainingRecords of currentBlock to be written at outDataBlock */
				memcpy(outDataBlock.data + outDataOffset,inDataBlocks[firstGroup].data + groupsArray[firstGroup].currentOffset,remainingRecords * RECORD_SIZE );
				outDataOffset += remainingRecords * RECORD_SIZE;
				outBlockFullness += remainingRecords;
				/* Go to next block */
				CALL_OR_ERROR5(BF_UnpinBlock(inDataBlocks[firstGroup].block));
				groupsArray[firstGroup].currentBlock ++ ;
				CALL_OR_ERROR5(BF_GetBlock(inputFileDesc,groupsArray[firstGroup].currentBlock,inDataBlocks[firstGroup].block));
				inDataBlocks[firstGroup].data = BF_Block_GetData(inDataBlocks[firstGroup].block);
				groupsArray[firstGroup].currentOffset = sizeof(int);
			}
			else{ /* We write only those record that there is space for them */
				memcpy(outDataBlock.data + outDataOffset,inDataBlocks[firstGroup].data + groupsArray[firstGroup].currentOffset,recordsSpace * RECORD_SIZE );
				outDataOffset += recordsSpace * RECORD_SIZE;
				outBlockFullness += recordsSpace;
				groupsArray[firstGroup].currentOffset += recordsSpace * RECORD_SIZE;
			}

			if( outBlockFullness == MAX_RECORDS ){
				BF_Block_SetDirty(outDataBlock.block);
				CALL_OR_ERROR5(BF_UnpinBlock(outDataBlock.block));
				CALL_OR_ERROR5(BF_AllocateBlock(outputFileDesc,outDataBlock.block));
				outDataBlock.data = BF_Block_GetData(outDataBlock.block);
				/* For all blocks except last, counter will be always MAX_RECORDS */
				memcpy(outDataBlock.data,&x,sizeof(int));
				outBlockFullness = 0;
				outDataOffset = sizeof(int);
			}

		}

		/* We are at last block of this group so we should check if it is the lastGroup */
		if( firstGroup == ( thisMerge.beginBlock + thisMerge.numberOfGroups ) ){ /* this is the last block of last group so it has lastGroupBlocks which may not be MAX_RECORDS */
			remainingRecords = ( (thisMerge.lastBlockRecords * RECORD_SIZE) - ( groupsArray[firstGroup].currentOffset - sizeof(int) ) ) / RECORD_SIZE; /* remaining records to copy from lastBlock */
		}
		else{ /* As we are not at the last group, last block has MAX_RECORDS */
			remainingRecords = ( (MAX_RECORDS * RECORD_SIZE) - ( groupsArray[firstGroup].currentOffset - sizeof(int) ) ) / RECORD_SIZE; /* remaining records to copy from lastBlock */
		}
		recordsSpace = MAX_RECORDS - outBlockFullness; /* how many records we can write in the outDataBlock before we allocate new block */

		if( recordsSpace >= remainingRecords ){ /* There is space for remainingRecords of last Block to be written at outDataBlock */
			memcpy(outDataBlock.data + outDataOffset,inDataBlocks[firstGroup].data + groupsArray[firstGroup].currentOffset,remainingRecords * RECORD_SIZE );
			outDataOffset += remainingRecords * RECORD_SIZE;
			outBlockFullness += remainingRecords;

			/* Write correct counter of the last block */
			memcpy(outDataBlock.data,&(thisMerge.lastBlockRecords),sizeof(int));
		}
		else{ /* We write only those record, there is space for them */
			memcpy(outDataBlock.data + outDataOffset,inDataBlocks[firstGroup].data + groupsArray[firstGroup].currentOffset,recordsSpace * RECORD_SIZE );
			outDataOffset += recordsSpace * RECORD_SIZE;
			outBlockFullness += recordsSpace;
			groupsArray[firstGroup].currentOffset += recordsSpace * RECORD_SIZE;
			remainingRecords -= recordsSpace;

			/* And we allocate new block so as to write the last remainingRecords */
			BF_Block_SetDirty(outDataBlock.block);
			CALL_OR_ERROR5(BF_UnpinBlock(outDataBlock.block));
			CALL_OR_ERROR5(BF_AllocateBlock(outputFileDesc,outDataBlock.block));
			outDataBlock.data = BF_Block_GetData(outDataBlock.block);
			/* Write correct counter of the last block this time */
			memcpy(outDataBlock.data,&(thisMerge.lastBlockRecords),sizeof(int));
			outBlockFullness = 0;
			outDataOffset = sizeof(int);
			recordsSpace = MAX_RECORDS;

			memcpy(outDataBlock.data + outDataOffset,inDataBlocks[firstGroup].data + groupsArray[firstGroup].currentOffset,remainingRecords * RECORD_SIZE );
			outDataOffset += remainingRecords * RECORD_SIZE;
			outBlockFullness += remainingRecords;
		}
		/* Unpin and Destroy firstGroup block  */
		CALL_OR_ERROR5(BF_UnpinBlock(inDataBlocks[firstGroup].block));
		BF_Block_Destroy(&(inDataBlocks[firstGroup].block));
		inDataBlocks[firstGroup].data = NULL;
	}

	/* Unpin and Destroy outDataBlock */
	BF_Block_SetDirty(outDataBlock.block);
	CALL_OR_ERROR5(BF_UnpinBlock(outDataBlock.block));
	BF_Block_Destroy(&(outDataBlock.block));

	free(inDataBlocks);
	free(groupsArray);

	return 1;

}

BF_ErrorCode getMergeRecord(int inputFileDesc,dataBlock *thisDataBlock,groupInfo *thisGroupInfo,int lastBlockRecords,int lastBlock){

	int value;

	thisDataBlock->record = Get_Record(thisDataBlock->data,thisGroupInfo->currentOffset);

	if( thisGroupInfo->currentBlock == lastBlock ){ /* We are at the last block of this merge */
		if( thisGroupInfo->currentOffset < (sizeof(int) + ( RECORD_SIZE * (lastBlockRecords-1) )) ){ /* Go to next record in currentBlock */
			value = 1;
		}
		else{
			/* There is nothing next, this group is finished! */
			value = -1;
		}
	}
	else if( thisGroupInfo->currentBlock == thisGroupInfo->maxBlock ){
		if( thisGroupInfo->currentOffset < (sizeof(int) + ( RECORD_SIZE * (MAX_RECORDS-1) )) ){ /* Go to next record in currentBlock */
			value = 1;
		}
		else{
			/* There is nothing next, this group is finished! */
			value = -1;
		}
	}
	else{
		if( thisGroupInfo->currentOffset < (sizeof(int) + ( RECORD_SIZE * (MAX_RECORDS-1) )) ){ /* Go to next record in currentBlock */
			value = 1;
		}
		else{
			/* Go to next block */
			value = 2;
		}
	}

	/* Do preparation for next Record */
	switch (value) {
		case 1:	/* Go to next record in currentBlock */
				thisGroupInfo->currentOffset += RECORD_SIZE;
				break;
		case 2:	/* Go to next block */
				CALL_OR_ERROR8(BF_UnpinBlock(thisDataBlock->block));
				thisGroupInfo->currentBlock ++ ;
				CALL_OR_ERROR8(BF_GetBlock(inputFileDesc,thisGroupInfo->currentBlock,thisDataBlock->block));
				thisDataBlock->data = BF_Block_GetData(thisDataBlock->block);
				thisGroupInfo->currentOffset = sizeof(int);
				break;
		case -1:/* There is nothing next so make currentBlock = -1 and free the necessaries */
				CALL_OR_ERROR8(BF_UnpinBlock(thisDataBlock->block));
				BF_Block_Destroy(&(thisDataBlock->block));
				thisGroupInfo->currentBlock = -1;
				thisDataBlock->data = NULL;
				break;
	}

	return BF_OK;
}

int copyAtEnd(int fileDesc1,int fileDesc2,int beginBlock,int endBlock){
	char *data1,*data2;
	int i;
	BF_Block *block1,*block2;

	BF_Block_Init(&block1);
	BF_Block_Init(&block2);

	for( i = beginBlock; i <= endBlock; i++ ){

		CALL_OR_ERROR6(BF_GetBlock(fileDesc1,i,block1));
		CALL_OR_ERROR6(BF_AllocateBlock(fileDesc2,block2));
		data1 = BF_Block_GetData(block1);
		data2 = BF_Block_GetData(block2);

		memcpy(data2,data1,BF_BLOCK_SIZE);

		CALL_OR_ERROR6(BF_UnpinBlock(block1));
		BF_Block_SetDirty(block2);
		CALL_OR_ERROR6(BF_UnpinBlock(block2));

	}
	BF_Block_Destroy(&block1);
	BF_Block_Destroy(&block2);

	return 1;
}

int compare(Record r1,Record r2,int fieldNo){ /* return values: 1 -> r1 >= r2 +++++ 0 -> r2 > r1 +++++ -1 -> error */
	int result;

	switch (fieldNo) {
		case 0:
				return r1.id <= r2.id;
		case 1:
				result = strcmp(r1.name,r2.name);
				return result <= 0;
		case 2:
				result = strcmp(r1.surname,r2.surname);
				return result <= 0;
		case 3:
				result = strcmp(r1.city,r2.city);
				return result <= 0;
		default:
				return -1;
	}

}
