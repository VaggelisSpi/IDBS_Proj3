#include "sort_file.h"
#include "bf.h"
#include "Inplace_Sorting.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#define CALL_OR_ERROR(call)     \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK) {      \
			return -1;           \
		}                         \
	}

#define CALL_OR_ERROR2(call)     \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK) {      \
			BF_Block_Destroy(&block); \
			return -1;           \
		}                         \
	}

/* unpin all the blocks that were used to load the next group of blocks */
int Unpin_Blocks(int temp_fileDesc, int beg, int end) {

	int i;
	BF_Block* block;

	BF_Block_Init(&block);

	for (i = beg; i <= end; i++) {
		CALL_OR_ERROR2(BF_GetBlock(temp_fileDesc, i, block));
		BF_Block_SetDirty(block);
		BF_UnpinBlock(block);
	}

	BF_Block_Destroy(&block);
	return 0;

}

/* Determines which blocks will be sorted */
int inPlace_Sorting(int temp_fileDesc, int bufferSize, int fieldNo) {

	int i, blocks_num, last_block_tupples;
	int beg = 1;
	int end;
	int last = 0; /* last == 1  means that we are gonna sort the last blocks of the file */

	BF_Block* block;
	BF_Block_Init(&block);

	CALL_OR_ERROR2(BF_GetBlockCounter(temp_fileDesc, &blocks_num));

	/* Ιn case the blocks of the file are less than buffer size then end will be the last block
		else it's gonna be the buffer size block */
	if (blocks_num - 1 > bufferSize) {
		end = bufferSize;
	}
	else {
		end = blocks_num - 1;
		last = 1;
	}

	CALL_OR_ERROR2(BF_GetBlock(temp_fileDesc, end, block));
	char* data = BF_Block_GetData(block);
	memcpy(&last_block_tupples, data, sizeof(int));

	int left = 0;
	int right = MAX_RECORDS*(end - beg) + last_block_tupples - 1;

	while (end <= blocks_num - 1) {

		Quick_Sort(temp_fileDesc, beg, end, left, right, fieldNo);
		Unpin_Blocks(temp_fileDesc, beg, end);

		beg = end + 1;
		end += bufferSize;
		if (last == 1) break;	/* if last == 1 means that we just sorted the last group */

		/* When end is equal to the number of blocks of the file, means we reached the final block */
		if (end >= blocks_num) {
			end = blocks_num - 1;
			last = 1;
		}

		CALL_OR_ERROR2(BF_GetBlock(temp_fileDesc, end, block));
		data = BF_Block_GetData(block);
		memcpy(&last_block_tupples, data, sizeof(int));
		left = 0;
		right = MAX_RECORDS*(end - beg) + last_block_tupples - 1;

	}
	BF_Block_Destroy(&block);
	return 1;
}

/* Implements Quick sort. Spliting the blocks in 2 parts based on a middle value determined by Partition and then sorts the 2 parts */
int Quick_Sort(int temp_fileDesc, int beg, int end, int left, int right, int fieldNo) {

	int m;

	if (left < right) {
		m = Partition(temp_fileDesc, beg, end, left, right, fieldNo);
		Quick_Sort(temp_fileDesc, beg, end, left, m-1, fieldNo);
		Quick_Sort(temp_fileDesc, beg, end, m+1, right, fieldNo);
	}

	return 1;
}

/* Gets the record pointed by left and finds its final position between the blocks beg and end */
int Partition(int temp_fileDesc, int beg, int end, int left, int right, int fieldNo) {

	Record pivot, current;
	int field_offs, offs, field_size, last_swap_offs, last_swap_block, pivot_offs, i;
	int m = left;
	int init_left = left;
	int left_block = left/MAX_RECORDS + beg;  /* the block left is */
	int pivot_block = left_block;   /* holds the block the pivot record is */
	BF_Block *block;
	char* data, *last_swap_data, *pivot_data;
	BF_Block_Init(&block);


	//left now has which record pivot is in the left block instead of all the blocks
	left -= (left_block - beg)*MAX_RECORDS;

	CALL_OR_ERROR2(BF_GetBlock(temp_fileDesc, left_block, block));
	data = BF_Block_GetData(block);
	last_swap_data = data;
	pivot_data = data;

	offs = sizeof(int) + left*RECORD_SIZE;  //offset for the left value in the block it is in
	pivot = Get_Record(data, offs);
	pivot_offs = offs;
	offs += RECORD_SIZE;
	last_swap_offs = offs;
	last_swap_block = left_block;


	left++;
	//if pivot was the last record of a block, then the next we'll swap will be the first of the next block
	if ( (left >= MAX_RECORDS) && (left - 1 != right) && (left_block < end) ) {
		last_swap_offs = sizeof(int);
		last_swap_block = left_block + 1;
		CALL_OR_ERROR2(BF_GetBlock(temp_fileDesc, last_swap_block, block));
		last_swap_data = BF_Block_GetData(block);
	}

	//if left is same as right we will swap left with itself
	if (left - 1 == right) {
		last_swap_offs -= RECORD_SIZE;
	}

	for (i = init_left; i < right; i++) {
		//if we look all the recordds in a block we have to check the next block
		if (left >= MAX_RECORDS) {
			left_block++;
			CALL_OR_ERROR2(BF_GetBlock(temp_fileDesc, left_block, block));
			data = BF_Block_GetData(block);
			left = 0;
			offs = sizeof(int);
		}
		//get each reord and compare it with pivot
		current = Get_Record(data, offs);
		if ( compare(current, pivot, fieldNo) ) {
			if (swap(&last_swap_data, &last_swap_offs, &last_swap_block, data, offs, temp_fileDesc, end) != 0){
				return -1;	//AN error occured
			}
			m++;
		}
		offs += RECORD_SIZE;
		left++;
	}

	if( m > init_left ){ /*Else it means that pivot is at the right position so we dont need to swap it */
		int final_block = m/MAX_RECORDS + beg;
		int final_pos =  m - (final_block - beg)*MAX_RECORDS;
		int final_offs = sizeof(int) + final_pos*RECORD_SIZE;
		CALL_OR_ERROR2(BF_GetBlock(temp_fileDesc, final_block, block));
		char* final_data = BF_Block_GetData(block);
		swap(&pivot_data, &pivot_offs, &pivot_block, final_data, final_offs, temp_fileDesc, end);
	}

	BF_Block_Destroy(&block);
	return m;
}

/* Gets the record at the block pointed by data which is at offset offs from the beggining */
Record Get_Record(char* data, int offs) {
	Record record;

	memcpy(&(record.id), data+offs, sizeof(record.id));
	offs += sizeof(record.id);

	memcpy(record.name, data+offs, sizeof(record.name));
	offs += sizeof(record.name);

	memcpy(record.surname, data+offs, sizeof(record.surname));
	offs += sizeof(record.surname);

	memcpy(record.city, data+offs, sizeof(record.city));

	return record;
}

/* Swaps the last swap record with our current record */
int swap(char** last_swap_data, int* last_swap_offs, int* last_swap_block, char* data, int offs, int temp_fileDesc, int end) {
	Record tmp;
	BF_Block* block;
	BF_Block_Init(&block);
	int field_offs = 0;

	//Put last swap record at a temp variable
	tmp = Get_Record(*last_swap_data, *last_swap_offs);
	//Copy our current record at the place of last swap record
	memcpy(*last_swap_data + *last_swap_offs, data + offs, sizeof(tmp.id) );
	field_offs += sizeof(tmp.id);

	memcpy(*last_swap_data + *last_swap_offs + field_offs, data + offs + field_offs, sizeof(tmp.name) );
	field_offs += sizeof(tmp.name);

	memcpy(*last_swap_data + *last_swap_offs + field_offs, data + offs + field_offs, sizeof(tmp.surname) );
	field_offs += sizeof(tmp.surname);

	memcpy(*last_swap_data + *last_swap_offs + field_offs, data + offs + field_offs, sizeof(tmp.city) );

	//Copy last swap record at the place of our current record
	field_offs = 0;
	memcpy(data + offs, &(tmp.id), sizeof(tmp.id) );
	field_offs += sizeof(tmp.id);

	memcpy(data + offs + field_offs, tmp.name, sizeof(tmp.name) );
	field_offs += sizeof(tmp.name);

	memcpy(data + offs + field_offs, tmp.surname, sizeof(tmp.surname) );
	field_offs += sizeof(tmp.surname);

	memcpy(data + offs + field_offs, tmp.city, sizeof(tmp.city) );

	*last_swap_offs += RECORD_SIZE;

	//if  we are at the end of the block then the next swap will have to be in the next block
	if ( (*last_swap_offs - sizeof(int)) / RECORD_SIZE == MAX_RECORDS && *last_swap_block < end) {
		(*last_swap_block)++;
		CALL_OR_ERROR2(BF_GetBlock(temp_fileDesc, *last_swap_block, block));
		*last_swap_data = BF_Block_GetData(block);
		*last_swap_offs = sizeof(int);
	}

	BF_Block_Destroy(&block);
	return 0;
}
