from sys import argv
from socket import *
from re import compile, match

# Define constants
LOCALHOST='127.0.0.1'
SERV_IP='10.10.1.2'
SERV_PORT=10000
IP_REG_EX=compile('\b(?:\d{1,3}\.){3}\d{1,3}\b')
PAYLOAD_SIZE=976


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
        payload_size = len(payload)
        # If reading is completed terminate
        if(not payload):
          break
        # Send 3 byte payload size information
        sock.sendall(payload_size.to_bytes(3, byteorder='little'))
        # Send packet
        sock.sendall(payload)
        raw_packets_sent+=1
        print("Payload size:", payload_size)
      print("Sending payloads completed. {} payloads sent.".format(raw_packets_sent))
  finally:
    sock.close()
    print("Socket is closed.")

if __name__ == "__main__":
  main(argv[1:])