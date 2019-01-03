from sys import argv
from time import *
from socket import *
from threading import *
from packet import *
from ntp import *
from select import *

# ready_buffer = {}
packet_buffer = {}
buffer_lock=Lock()
seq_num = 0
shared_number_lock=Lock()
# largest_ack_given=0
# shared_largest_ack_lock=Lock()

def printState():
  # keys=list(ready_buffer.keys())
  # print("Ready buffer keys: {}, length:{}".format(keys,len(keys)))
  keys=list(packet_buffer.keys())
  print("Packet buffer keys: {}, length:{}".format(keys,len(keys)))
  # print("Sequence number: {}".format(seq_num))

# def setGivenACK(ack):
#   global largest_ack_given
#   if(ack>largest_ack_given):
#     largest_ack_given=ack

# TODO: correct this function
# def checkGap():
#   try:
#     with buffer_lock:
#       gap_exists=False
#       sorted_seq_nums=sorted(packet_buffer.keys())
#       with shared_largest_ack_lock:
#         if(largest_ack_given!=sorted_seq_nums[0]):
#           return (-1, gap_exists)
#       number_of_packets=len(sorted_seq_nums)
#       number_of_iterations=number_of_packets-1
#       i=0
#       # print("{} in checkGap {}".format(current_thread().name,sorted_seq_nums))
#       while(i<number_of_iterations):
#         seq_num1 = sorted_seq_nums[i]
#         pay_len = packet_buffer[seq_num1][1]
#         seq_num2 = sorted_seq_nums[i+1]
#         if(seq_num1+pay_len!=seq_num2):
#           gap_exists=True
#           break
#         ready_buffer[seq_num1]=packet_buffer[seq_num1][0]
#         del packet_buffer[seq_num1]
#         i+=1
#       if(i<number_of_packets):
#         seq=sorted_seq_nums[i]
#         ack_num=(seq+packet_buffer[seq][1]) % MAX_ALLOWED_SEQ_NUM
#         ready_buffer[seq]=packet_buffer[seq][0]
#         del packet_buffer[seq]
#         return (ack_num, gap_exists)
#       return (-1, gap_exists)
#   except Exception as e:
#     print("[{}]: EXCEPTION IN checkGap: {}".format(current_thread().name,e)) 


def storePacketOnBuffer(seq_num, payload, pay_len):
  with buffer_lock:
    # print("{} in storePacketOnBuffer inserting {}".format(current_thread().name, seq_num))
    # If the packet is new
    if(seq_num not in packet_buffer):
      packet_buffer[seq_num]=payload
    else:
      print("[{}]: Retransmission of {} ignored.".format(current_thread().name, seq_num))

def reconstructAndSave():
  if(packet_buffer):
    print("Saving file...")
    sorted_seq_nums=sorted(packet_buffer.keys())
    with open('output.txt','wb') as file:
      for seq_num in sorted_seq_nums:
        file.write(packet_buffer[seq_num])
    packet_buffer.clear()

def prepareResponse(ack_num):
  global seq_num
  # Epoch time related calculations 
  integer, fraction = str(time()).split('.')
  integer=convertBytesOfLength(int(integer), MAX_INTEGER)
  fraction=convertBytesOfLength(int(fraction), MAX_DECIMAL)
  payload='.'.encode()
  # Timestamp of arrival is passed as payload at every turn 
  payload=integer+payload+fraction
  payload_len=len(payload)
  # Create packet
  with shared_number_lock:
    response=packetize(seq_num, ack_num, len(payload), payload)
    # Calculate next seq_num
    seq_num=seq_num + payload_len
  return response

def router_handler(addr, completed):
  sock = socket(AF_INET, SOCK_DGRAM)
  sock.bind(addr)
  print('[{}]: Listening to port:{}'.format(current_thread().name, addr[1]))
  # seq_num = 0
  ack_num = 0
  packets_received=0
  try:
    while True:
      packet, address = sock.recvfrom(MAX_PACKET_SIZE)
      # Parse
      parsed=parsePacket(packet)
      # Check if intact
      if(parsed is None):
        continue
      # Check if EMPTY_PACKET, special case indicating that connection is over
      if(packet==EMPTY_PACKET):
        print("EMPTY PACKET RECEIVED")
        sock.sendto(EMPTY_PACKET, address)
        print("[{}]: Empty packet is sent".format(current_thread().name))
        # print("[{}]: Connection is over".format(current_thread().name))
        # This check is required for the retransmission of the empty packet
        if(packets_received):
          completed.wait()
          print("[{}]: After barrier".format(current_thread().name))
          ack_num = 0
          packets_received=0
        continue
      packets_received+=1
      r_seq_num, r_ack_num, r_payload_len, r_payload=parsed[1:]
      print("[{}]: Retrieved packet: {}".format(current_thread().name, r_seq_num))
      storePacketOnBuffer(r_seq_num, r_payload, r_payload_len)
      print("[{}]: Packets on packet buffer: {}".format(current_thread().name, len(packet_buffer)))
      given_ack=r_seq_num+r_payload_len
      response=prepareResponse(given_ack)
      sock.sendto(response, address)
      print("[{}]: Given ACK: {}\n".format(current_thread().name, given_ack))
      # print("[{}]: Packet with ack {} is sent".format(current_thread().name, ack_num))
  except Exception as e:
    print("[{}]: EXCEPTION: {}".format(current_thread().name,e))
  finally:
    sock.close()
    print("[{}]: Socket is closed.".format(current_thread().name))

def main(argv):
  # Define IP & port number of the server
  TH1_IP = '10.10.3.2'
  TH2_IP = '10.10.5.2'
  argc=len(argv)
  if(argc>0):
    TH1_IP=''
    TH2_IP=''
  UDP_TH1_PORT = 5000 # Communicate with r1
  UDP_TH2_PORT = 5001 # Communicate with r2
  UDP_addr1 = (TH1_IP, UDP_TH1_PORT) # Address of d (main thread) that r1 will use
  UDP_addr2 = (TH2_IP, UDP_TH2_PORT) # Address of d (worker thread) that r2 will use
  # Initialize and start threads
  completed=Barrier(3)
  th1=Thread(target=router_handler, args=(UDP_addr1, completed))
  th2=Thread(target=router_handler, args=(UDP_addr2, completed))
  th1.start()
  th2.start()
  try:
    while True:
      completed.wait()
      reconstructAndSave()
      # seq_num = 0
      # ack_num = 0
      # packets_received=0
      # receiving=True
      # gap_exists=True
      # largest_ack_given=0
      completed.reset()
  except Exception as e:
    print("[{}]: EXCEPTION: {}".format(current_thread().name,e))    
  finally:
    sock.close()
    print("[MainThread]: Socket is closed.")
    th1.join()
    th2.join()
    print("[MainThread]: is terminated.")

if __name__ == "__main__":
    main(argv[1:])