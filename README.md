# IDBS_Proj3

Implementation of merge sort for a data base system.


In order to compile and run the code youll need to move to the `sorted_file_64` file
```
cd sorted_file_64
```

Compile all 3 with 
```
make
```

1. Creates the db and inserts the data into them.
```
make sr_main1
./build/sr_main1
```
2. Orders the data
```
make sr_main2
./build/sr_main2
```
3. Prints the data and deletes the db.
```
make sr_main3
./build/sr_main3
```
In order to delete the creates files run
```
make clean
```
