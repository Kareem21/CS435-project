### CS435 Big Data Project
Analyzing museum datasets (paintings) to determine how the aspect ratio of paintings by different artists changes it's orientation over time. 

For Artworks.csv	  
 - If String line is the line you're currently on , the year is stored in  line[8].  It's either an integer like this 1968 or a range like this 1890 - 1891  so it might be better to store it as a string then convert it to an int later.  We could also average the range and return one year instead of a range.
-Artist will be stored in line[1] (the 2nd element on the line)
- Dimensions will be at line[10] and it's stored in this format "13 1/2 x 12 1/2". Maybe store dimensions in a string since it has 'x' in it? 
- Some lines are photographs instead of paintings

