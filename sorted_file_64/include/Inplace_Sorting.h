#ifndef INPLACE_H
#define INPLACE_H

int Unpin_Blocks(int temp_fileDesc, int beg, int end);

int inPlace_Sorting(int temp_fileDesc, int bufferSize, int fieldNo);

int Partition(int temp_fileDesc, int beg, int end, int left, int right, int fieldNo);

int Quick_Sort(int temp_fileDesc, int beg, int end, int left, int right, int fieldNo);

Record Get_Record(char* data, int offs);

int swap(char** last_swap_data, int* last_swap_offs, int* last_swap_block, char* data, int offs, int temp_fileDesc, int end);

#endif // INPLACE_H
