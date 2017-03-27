from __future__ import print_function

import sys
import yarntf

server = yarntf.createTrainServer()

print('Number of arguments: ' + str(len(sys.argv)))
print('Argument list: ' + str(sys.argv))
