from sys import argv
from time import *
from socket import *
from collections import OrderedDict
from packet import *
from ntp import *
from re import compile, match

# Define fixed window size
WINDOW_SIZE=1024
LOCALHOST='127.0.0.1'
SERV_IP='10.10.1.2'
IP_REG_EX=compile('\b(?:\d{1,3}\.){3}\d{1,3}\b')

def main(argv):
  global SERV_IP
  # Create a TCP/IP socket
  sock = socket(AF_INET, SOCK_STREAM)
  try:
    if(len(argv)>0):
      IP=argv[0]
      if(IP.lower()=="localhost"):
        SERV_IP=LOCALHOST
      elif(match(IP_REG_EX, IP)):
        SERV_IP=IP
      else:
        print("Input format for IP is wrong!")
    # Connect the socket to the port where the B is listening
    server_address = (SERV_IP, 10000)#link-0
    print('Connecting to {} port {}'.format(*server_address))
    sock.connect(server_address)
    with open('input.txt','rb') as file:
      packet_buffer = OrderedDict()
      upload_start_time = 0
      upload_finish_time = 0
      next_seq_num = 0
      ack_num = 0
      packets_sent=0
      payload = None
      sending=True
      while(sending):
        try:
          base=next_seq_num
          # Event 1: Send all packets in the window
          while(packets_sent<WINDOW_SIZE):
            # Read from file
            payload = bytearray(file.read(PAYLOAD_SIZE))
            # If reading is completed terminate
            if(not payload):
              sending=False
              break
            # Get how many bytes read from file
            actual_payload_size = len(payload)
            # Create packet
            packet = packetize(next_seq_num, ack_num, actual_payload_size, payload)
            print("Event 1: Packet with sequence number: {} sent".format(next_seq_num))
            # Find next sequence number
            next_seq_num = (next_seq_num + actual_payload_size) % MAX_ALLOWED_SEQ_NUM
            # Find and keep the expected ack number along with the packet just sent 
            expected_ack_num = next_seq_num
            packet_buffer[expected_ack_num]=packet
            # Send packet
            packets_sent+=1
            sock.sendall(packet)
            # For base start the timer
            if next_seq_num==base:
              sock.settimeout(0.1)# Seconds
            # Save the time when upload starts
            if next_seq_num==0:
              upload_start_time = time()+offset
          # Wait for a response
          response=sock.recv(MAX_PACKET_SIZE)
          # Event 2: Packet recieved
          # Parsing and checking for errors
          parsed=parsePacket(response)
          if(parsed is None):
            continue
          r_next_seq_num, r_ack_num, r_payload_len, r_payload=parsed[1:]
          print("Event 1: Packet with ack number: {} received".format(r_ack_num))
          # If ack is received for the base, disable timeout
          expected_base_ack=next(iter(packet_buffer))
          if(expected_base_ack==r_ack_num):
            sock.settimeout(None)
          # Expecting cumulative acks
          # Mark packets correctly received (if any) and remove them from buffer
          expected_acks=list(packet_buffer.keys())
          for expected_ack in expected_acks:
            if(expected_ack<=r_ack_num):
              del packet_buffer[expected_ack]
              packets_sent-=1
            else:
              break
        # Event 3: Timeout
        except timeout:
            print("Event 3: Timeout triggered, resending the first packet")
            expected_base_ack=next(iter(packet_buffer))
            packet=packet_buffer[expected_base_ack]
            # Send packet
            sock.sendall(packet)
            sock.settimeout(0.1)# in seconds
  finally:
    sock.close()
    print("Socket is closed.")

if __name__ == "__main__":
  main(argv[1:])