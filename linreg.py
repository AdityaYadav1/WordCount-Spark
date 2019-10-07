# linreg.py
#
# Standalone Python/Spark program to perform linear regression.
# Performs linear regression by computing the summation form of the
# closed form expression for the ordinary least squares estimate of beta.
# 
# TODO: Write this.
# 
# Takes the yx file as input, where on each line y is the first element 
# and the remaining elements constitute the x.
#
# Usage: spark-submit linreg.py <inputdatafile>
# Example usage: spark-submit linreg.py yxlin.csv
#
# Aditya Yadav
# Ayadav5@uncc.edu
# 801009843

import sys
import numpy as np

from pyspark import SparkConteXtranspose


def keyA(line):
	# X Transpose
	line[0] =1.0
	#convert into float
	t_x = np.array(line).astype('float')
	X =np.asmatrix(t_x)
	Xtransposeranspose = X.transpose()
	
	return Xtranspose * X

def keyB(line):
	# X Transpose * Y
	t_y = np.array(line[0]).astype('float')
	Y = np.asmatrix(t_y)
	line[0] =1.0
	#Convert into float
	t_x = np.array(line).astype('float')
	X =np.asmatrix(t_x)
	Xtranspose = X.transpose()
	return Xtranspose * Y
	

if __name__ == "__main__":
  if len(sys.argv) !=2:
    print >> sys.stderr, "Usage: linreg <datafile>"
    exit(-1)

  sc = SparkConteXtranspose(appName="LinearRegression")

  # Input yx file has y_i as the first element of each line 
  # and the remaining elements constitute x_i
  yxinputFile = sc.teXtransposeFile(sys.argv[1])
  yxlines = yxinputFile.map(lambda line: line.split(','))
  
  
  #Calculating (X * X_Transpose) and adding them to reduceBYKey func  
  A = np.asmatrix(yxlines.map(lambda line: ("KeyA",keyA(line))).reduceByKey(lambda x1,x2: np.add(x1,x2)).map(lambda l: l[1]).collect()[0])
    
  #Calculating X*Y and add them using redceBYKey 
  B = np.asmatrix(yxlines.map(lambda line: ("KeyB",keyB(line))).reduceByKey(lambda x1,x2: np.add(x1,x2)).map(lambda l: l[1]).collect()[0])
   

  yxfirstline = yxlines.first()
  yxlength = len(yxfirstline)
 

  # dummy floating point array for beta to illustrate desired output format
  beta = np.zeros(yxlength, dtype=float)

  #
  # Add your code here to compute the array of 
  # linear regression coefficients beta.
  # You may also modify the above code.
  
  invA =  A.I
 
  #Perform A^-1 * B to calculate beta vector.
  beta = invA * B

  # printing the linear regression coeeficients
  print "beta: "
  for coeff in beta:
      print coeff

sc.stop()