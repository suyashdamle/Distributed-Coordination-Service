from Node import Node
from Message import Message
from coordination_utils import *
import sys 	

# start the node here
# print(type(sys.argv[1]))
# print(type(int(sys.argv[1])))
node = Node(port = int(sys.argv[1]), is_leader = bool(int(sys.argv[2])))