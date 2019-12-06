import matplotlib.pyplot as plt
import numpy as np

x,y= np.loadtxt('o.txt', delimiter='\t', unpack=True)


plt.plot(x,y, marker='o')

plt.title('Data from the CSV File: People and Expenses')

plt.xlabel('Years Since First Painting')
plt.ylabel('Composition Ratio')

plt.show()
