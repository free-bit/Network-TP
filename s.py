from sys import argv
from time import *
from socket import *
from struct import pack, unpack
from re import compile, match
from packet import *
from ntp import *

# Define constants
LOCALHOST='127.0.0.1'
SERV_IP='10.10.1.2'
SERV_PORT=10000
IP_REG_EX=compile('\b(?:\d{1,3}\.){3}\d{1,3}\b')

def main(argv):
  global SERV_IP, SERV_PORT
  # Create a TCP/IP socket
  sock = socket(AF_INET, SOCK_STREAM)
  argc=len(argv)
  if(argc>0):
    if(argc>=1):
      IP=argv[0]
      if(IP.lower()=="localhost"):
        SERV_IP=LOCALHOST
      elif(match(IP_REG_EX, IP)):
        SERV_IP=IP
      else:
        print("Input format for IP is wrong!")
    if(argc>=2):
      SERV_PORT=int(argv[1])


  # Connect the socket to the port where the B is listening
  server_address = (SERV_IP, SERV_PORT)#link-0
  try:
    print('Connecting to {} port {}'.format(*server_address))
    sock.connect(server_address)
    with open('input.txt','rb') as file:
      upload_start_time = 0
      upload_finish_time = 0
      # next_seq_num = 0
      # ack_num = 0
      raw_packets_sent = 0
      while(1):
        # Read from file
        payload = bytearray(file.read(PAYLOAD_SIZE))
        # If reading is completed terminate
        if(not payload):
          break
        # Send packet
        sock.sendall(payload)
        # Save the time when upload starts
        if raw_packets_sent==0:
          upload_start_time = time()
        raw_packets_sent+=1
      # Wait for a message indicating the successful upload
      # print("Sending is completed. {} raw packets sent. Waiting for a response...".format(raw_packets_sent))
      # upload_finish_time = unpack('d', sock.recv(PAYLOAD_SIZE))[0]
      # print("Upload completed in ",upload_finish_time-upload_start_time,"seconds.")
  finally:
    sock.close()
    print("Socket is closed.")

if __name__ == "__main__":
  main(argv[1:])