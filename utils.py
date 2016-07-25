import string
import random
import socket

def randomstr(randomlength = 8):
	a = list(string.ascii_letters)
	random.shuffle(a)
	return ''.join(a[:randomlength])

def check_port_open(ip, port):
	sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sk.settimeout(1)
	isopen = False
	try:
		sk.connect((ip, port))
		isopen = True
	except Exception as e:
		isopen = False
	finally:
		sk.close()
	return isopen

