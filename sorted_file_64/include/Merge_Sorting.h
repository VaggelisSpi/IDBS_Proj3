#ifndef MERGE_H
#define MERGE_H

typedef struct mergeInfo{
	int beginBlock;
	int blocksInEachGroup;
	int numberOfGroups;
	int lastBlockRecords;
	int lastGroupBlocks;
} mergeInfo;

typedef struct groupInfo{
	int currentBlock;
	int currentOffset;
	int maxBlock;
} groupInfo;

typedef struct dataBlock{
	BF_Block *block;
	char *data;
	Record record;
} dataBlock;

int mergeSort(int inputFileDesc, const char *output_filename,int bufferSize, int fieldNo);
int mergeGroups(int inputFileDesc,int outputFileDesc,mergeInfo thisMerge,int bufferSize,int fieldNo);
BF_ErrorCode getMergeRecord(int inputFileDesc,dataBlock *thisDataBlock,groupInfo *thisGroupInfo,int lastBlockRecords,int lastBlock);
int copyAtEnd(int fileDesc1,int fileDesc2,int beginBlock,int endBlock);

#endif //MERGE_H
