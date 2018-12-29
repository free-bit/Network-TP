from sys import argv
from time import *
from socket import *
from threading import *
from packet import *
from ntp import *

ready_buffer = {}
packet_buffer = {}
buffer_lock=Lock()
worker_lock=Lock()
main_lock=Lock()
seq_num = 0
shared_number_lock=Lock()

def checkGap():
  with buffer_lock:
    gap_exists=False
    sorted_seq_nums=sorted(packet_buffer.keys())
    number_of_iterations=len(sorted_seq_nums)-1
    i=0
    # print("{} in checkGap {}".format(current_thread().name,sorted_seq_nums))
    while(i<number_of_iterations):
      seq_num1 = sorted_seq_nums[i]
      pay_len = packet_buffer[seq_num1][1]
      seq_num2 = sorted_seq_nums[i+1]
      if(seq_num1+pay_len!=seq_num2):
        gap_exists=True
        break
      ready_buffer[seq_num1]=packet_buffer[seq_num1][0]
      del packet_buffer[seq_num1]
      i+=1
    seq=sorted_seq_nums[i]
    ack_num=(seq+packet_buffer[seq][1]) % MAX_ALLOWED_SEQ_NUM
    ready_buffer[seq]=packet_buffer[seq][0]
    del packet_buffer[seq]
    # print(current_thread().name)
    # print("Generated ack:", ack_num)
    # print("Gap ?", gap_exists)
    return (ack_num, gap_exists)


def storePacketOnBuffer(seq_num, payload, pay_len):
  with buffer_lock:
    # print("{} in storePacketOnBuffer inserting {}".format(current_thread().name, seq_num))
    if(seq_num not in packet_buffer):
      packet_buffer[seq_num]=(payload, pay_len)

def reconstructAndSave():
  if(ready_buffer):
    print("Saving file...")
    sorted_seq_nums=sorted(ready_buffer.keys())
    with open('output.txt','wb') as file:
      for seq_num in sorted_seq_nums:
        file.write(ready_buffer[seq_num])
    ready_buffer.clear()

def prepareResponse(ack_num):
  global seq_num
  with shared_number_lock:
    # Epoch time related calculations 
    integer, fraction = str(time()).split('.')
    integer=convertBytesOfLength(int(integer), MAX_INTEGER)
    fraction=convertBytesOfLength(int(fraction), MAX_DECIMAL)
    payload='.'.encode()
    # Timestamp of arrival is passed as payload at every turn 
    payload=integer+payload+fraction
    payload_len=len(payload)
    # Create packet
    response=packetize(seq_num, ack_num, len(payload), payload)
    # Calculate next seq_num
    seq_num=(seq_num + payload_len) % MAX_ALLOWED_SEQ_NUM
    return response

def router2_handler(addr):
  worker_lock.acquire()
  sock = socket(AF_INET, SOCK_DGRAM)
  sock.bind(addr)
  print('[{}]: Listening to port:{}'.format(current_thread().name, addr[1]))
  # seq_num = 0
  ack_num = 0
  packets_received=0
  connected=True
  gap_exists=True
  try:
    while True:
      packets = []
      address = None
      # Poll the port
      # Disable blocking
      sock.setblocking(False)
      while True:
        # While not blocked (i.e. there are packets on the socket) get packet
        # print("[{}]: Polling...".format(current_thread().name))
        try:
          packet, address = sock.recvfrom(MAX_PACKET_SIZE)
          # Disable blocking
          sock.setblocking(False)
          packets.append(packet)
          # print("[{}]: Packets received: {}".format(current_thread().name, len(packets)))
        # If blocked (i.e. there aren't any packets on the socket)
        except BlockingIOError:
          # print("[{}]: Caught exception.".format(current_thread().name))
          # If at least one packet found
          if(packets):
            # print("[{}]: Polled...".format(current_thread().name))
            break
          # Enable blocking
          sock.setblocking(True)
          # Otherwise go back and wait
      # Iterate over received packets
      response = None
      for packet in packets:
        # Parse
        parsed=parsePacket(packet)
        # Check if intact
        if(parsed is None):
          continue
        # Check if EMPTY_PACKET, special case that marks the end 
        if(packet==EMPTY_PACKET):
          connected=False
          continue
        # Store intact packets
        packets_received+=1
        r_seq_num, r_ack_num, r_payload_len, r_payload=parsed[1:]
        storePacketOnBuffer(r_seq_num, r_payload, r_payload_len)
      # print("[{}]: Packets received correctly: {}".format(current_thread().name, packets_received))
      # Connection is over
      if(not gap_exists and not connected):
        sock.sendto(EMPTY_PACKET, address)
        print("[{}]: Connection is over".format(current_thread().name))
        ack_num = 0
        packets_received=0
        connected=True
        gap_exists=True
        worker_lock.release()
        main_lock.acquire()
        worker_lock.acquire()
        main_lock.release()
      else:
        # Check if there is any gap in the arrival of packets to packet_buffer
        ack_num, gap_exists=checkGap()
        response=prepareResponse(ack_num)
        sock.sendto(response, address)
        print("[{}]: Packet with ack {} is sent".format(current_thread().name, ack_num))
  finally:
    sock.close()
    print("[{}]: Socket is closed.".format(current_thread().name))

def main(argv):
  global seq_num
  main_lock.acquire()
  # Define IP & port number of the server
  IP = ''
  UDP_R1_PORT = 5000 # Communicate with r1
  UDP_R2_PORT = 5001 # Communicate with r2
  UDP_addr1 = (IP, UDP_R1_PORT) # Address of d (main thread) that r1 will use
  UDP_addr2 = (IP, UDP_R2_PORT) # Address of d (worker thread) that r2 will use
  # Create a new UDP socket in the main thread, bind the ip & port number 
  sock = socket(AF_INET, SOCK_DGRAM)
  sock.bind(UDP_addr1)
  print('[{}]: Listening to port:{}'.format(current_thread().name, UDP_addr1[1]))
  # Initialize and start threads
  worker_thread=Thread(target=router2_handler, args=(UDP_addr2,))
  worker_thread.start()
  ack_num = 0
  packets_received=0
  connected=True
  gap_exists=True
  try:
    while True:
      packets = []
      address = None
      # Poll the port
      # Disable blocking
      sock.setblocking(False)
      while True:
        # While not blocked (i.e. there are packets on the socket) get packet
        # print("[{}]: Polling...".format(current_thread().name))
        try:
          packet, address = sock.recvfrom(MAX_PACKET_SIZE)
          # Disable blocking
          sock.setblocking(False)
          packets.append(packet)
          # print("[{}]: Packets received: {}".format(current_thread().name, len(packets)))
        # If blocked (i.e. there aren't any packets on the socket)
        except BlockingIOError:
          # print("[{}]: Caught exception.".format(current_thread().name))
          # If at least one packet found
          if(packets):
            # print("[{}]: Polled...".format(current_thread().name))
            break
          # Enable blocking
          sock.setblocking(True)
          # Otherwise go back and wait
      # Iterate over received packets
      response = None
      for packet in packets:
        # Parse
        parsed=parsePacket(packet)
        # Check if intact
        if(parsed is None):
          continue
        # Check if EMPTY_PACKET, special case that marks the end 
        if(packet==EMPTY_PACKET):
          connected=False
          continue
        # Store intact packets
        packets_received+=1
        r_seq_num, r_ack_num, r_payload_len, r_payload=parsed[1:]
        storePacketOnBuffer(r_seq_num, r_payload, r_payload_len)
      # print("[{}]: Packets received correctly: {}".format(current_thread().name, packets_received))
      # Connection is over
      if(not gap_exists and not connected):
        sock.sendto(EMPTY_PACKET, address)
        print("[{}]: Connection is over".format(current_thread().name))
        worker_lock.acquire()
        reconstructAndSave()
        ack_num = 0
        packets_received=0
        connected=True
        gap_exists=True
        main_lock.release()
        worker_lock.release()
      else:
        # Check if there is any gap in the arrival of packets to packet_buffer
        ack_num, gap_exists=checkGap()
        response=prepareResponse(ack_num)
        sock.sendto(response, address)
        print("[{}]: Packet with ack {} is sent".format(current_thread().name, ack_num))
  finally:
    sock.close()
    print("[MainThread]: Socket is closed.")
    worker_thread.join()
    print("[Thread-1]: is terminated.")
    print("[MainThread]: is terminated.")

if __name__ == "__main__":
    main(argv[1:])