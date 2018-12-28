from sys import argv
from time import *
from socket import *
from threading import *
from packet import *
from ntp import *

packet_buffer = {}
lock=Lock()

def storeData(seq_num, payload):
  if(not packet_buffer[seq_num]):
    with lock:
      packet_buffer[seq_num]=payload

def reconstructAndSave():
  if(packet_buffer):
    print("Saving file...")
    sorted_seq_nums=sorted(packet_buffer.keys())
    with open('output.txt','wb') as file:
      for seq_num in sorted_seq_nums:
        file.write(packet_buffer[seq_num])
    packet_buffer.clear()

def prepareResponseTo(packet, seq_num, ack_num):
  parsed=parsePacket(packet)
  if(parsed is None):
    return None
  r_seq_num, r_ack_num, r_payload_len, r_payload=parsed[1:]
  if(r_seq_num==0 and r_ack_num==0 and r_payload_len==0):
    return (EMPTY_PACKET, 0, 0)
  # print("[{}]: Packet with sequence number: {} is received."
        # .format(current_thread().name, r_seq_num)) 
  # Correctly in order received, send a response
  if(ack_num==r_seq_num):
    # print("[{}]: In order arrival of the packet".format(current_thread().name)) 
    # Timestamp of arrival is passed as payload at every turn 
    # Time related calculations 
    integer, fraction = str(time()).split('.')
    integer=convertBytesOfLength(int(integer), MAX_INTEGER)
    fraction=convertBytesOfLength(int(fraction), MAX_DECIMAL)
    payload='.'.encode()
    payload=integer+payload+fraction
    payload_len=len(payload)
    ack_num=(r_seq_num + r_payload_len) % MAX_ALLOWED_SEQ_NUM
    print(current_thread().name, "prepared ack:", ack_num)
    response=packetize(seq_num, ack_num, len(payload), payload)
    seq_num=(seq_num + payload_len) % MAX_ALLOWED_SEQ_NUM
    return (response, seq_num, ack_num)
  # If the packet is new store it in buffer even though it is not received in order
  storeData(r_seq_num, r_payload)
  # However, don't send any reply to force sender to resend the older packets
  return None

def router2_handler(addr):
  sock = socket(AF_INET, SOCK_DGRAM)
  sock.bind(addr)
  print('[{}]: Listening to port:{}'.format(current_thread().name, addr[1]))
  seq_num = 0
  ack_num = 0
  packets_received=0
  try:
    while True:
      # Listen the port
      packet, address = sock.recvfrom(MAX_PACKET_SIZE)
      # Create a response if received packet is intact
      prepared=prepareResponseTo(packet, seq_num, ack_num)
      # If there is no response to send wait for a new packet
      if(prepared is None):
        continue
      packets_received+=1
      print("[{}]: Packets received: {}".format(current_thread().name, packets_received))
      # Otherwise send the response
      response, seq_num, ack_num = prepared
      # print(current_thread().name, "prepared ack:", ack_num)
      sock.sendto(response, address)
      # Last message resets the parameters
      if(response==EMPTY_PACKET):
        print("[{}]: Connection is over".format(current_thread().name))
        packets_received=0
  finally:
    sock.close()
    print("[{}]: Socket is closed.".format(current_thread().name))

def main(argv):
  # Define IP & port number of the server
  IP = ''
  UDP_R1_PORT = 5000 # Communicate with r1
  UDP_R2_PORT = 5001 # Communicate with r2
  UDP_addr1 = (IP, UDP_R1_PORT) # Address of d (main thread) that r1 will use
  UDP_addr2 = (IP, UDP_R2_PORT) # Address of d (worker thread) that r2 will use
  # Create a new UDP socket in the main thread, bind the ip & port number 
  sock = socket(AF_INET, SOCK_DGRAM)
  sock.bind(UDP_addr1)
  print('[MAIN THREAD]: Listening to port:{}'.format(UDP_addr1[1]))
  # Initialize and start threads
  worker_thread=Thread(target=router2_handler, args=(UDP_addr2,))
  worker_thread.start()
  packets_received=0
  seq_num = 0
  ack_num = 0
  try:
    while True:
      # Listen the port
      packet, address = sock.recvfrom(MAX_PACKET_SIZE)
      # Create a response if received packet is intact
      prepared=prepareResponseTo(packet, seq_num, ack_num)
      # If there is no response to send wait for a new packet
      if(prepared is None):
        continue
      packets_received+=1
      print("[MAIN THREAD]: Packets received:", packets_received)
      # Otherwise send the response
      response, seq_num, ack_num = prepared
      # print(current_thread().name, "prepared ack:", ack_num)
      sock.sendto(response, address)
      # Last message resets the parameters additionally reorder packets and save
      if(response==EMPTY_PACKET):
        print("[MAIN THREAD]: Connection is over")
        reconstructAndSave()
        packets_received=0
  finally:
    sock.close()
    print("[MAIN THREAD]: Socket is closed.")
    worker_thread.join()
    print("[WORKER THREAD]: is terminated.")
    print("[MAIN THREAD]: is terminated.")

if __name__ == "__main__":
    main(argv[1:])