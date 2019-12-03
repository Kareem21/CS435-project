CS435 Big Data Project


** For Artworks.csv
	  
 	- If String line is the line you're currently on , the year is stored in  line[8].  It's either an integer like this 1968 or a range like this 1890 - 1891  so it might be better to store it as a string then convert it to an int later.  
	-Artist will be stored in line[1] (the 2nd element on the line)
	- Dimensions will be at line[10] and it's stored in this format "13 1/2 x 12 1/2". Maybe store dimensions in a string since it has 'x' in it? 
 		Some of these will containt text like this "each 24 1/2 x 42 " or "plate 24  x 50"  so it might be a good idea to remove these since they are not paintings  