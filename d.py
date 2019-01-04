from sys import argv
from time import *
from socket import *
from threading import *
from packet import *
from ntp import *
from select import *

packet_buffer = {}
buffer_lock=Lock()
seq_num = 0
shared_number_lock=Lock()
NTP_OFFSET=0

# Place packet on the shared buffer
def storePacketOnBuffer(seq_num, payload, pay_len):
  with buffer_lock:
    # print("{} in storePacketOnBuffer inserting {}".format(current_thread().name, seq_num))
    # If the packet is new
    if(seq_num not in packet_buffer):
      packet_buffer[seq_num]=payload
    else:
      print("[{}]: Retransmission of {} ignored.".format(current_thread().name, seq_num))

# Main thread sorts packets on the shared buffer and stores on "output.txt"
def reconstructAndSave():
  if(packet_buffer):
    print("Saving file...")
    sorted_seq_nums=sorted(packet_buffer.keys())
    with open('output.txt','wb') as file:
      for seq_num in sorted_seq_nums:
        file.write(packet_buffer[seq_num])
    packet_buffer.clear()

# Worker threads prepare responses
# It needs to take and update seq_num
# and put timestamp on payload
def prepareResponse(ack_num):
  global seq_num
  # Epoch time related calculations 
  integer, fraction = str(time()-NTP_OFFSET).split('.')
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

# Worker thread implementation
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
      # Otherwise
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
  global NTP_OFFSET, seq_num
  # Define IP & port number of the server
  TH1_IP = '10.10.3.2'
  TH2_IP = '10.10.5.2'
  argc=len(argv)
  if(argc>0):
    if("--localhost" in argv):
      TH1_IP=''
      TH2_IP=''
    if("--ntp" in argv):
      NTP_OFFSET=getNTPTime()
  UDP_TH1_PORT = 5000 # Communicate with thread-1 of broker
  UDP_TH2_PORT = 5001 # Communicate with thread-2 of broker
  UDP_addr1 = (TH1_IP, UDP_TH1_PORT) # Address of thread-1 that thread-1 of broker will use
  UDP_addr2 = (TH2_IP, UDP_TH2_PORT) # Address of thread-2 that thread-2 of broker will use
  # Initialize and start threads
  completed=Barrier(3)
  th1=Thread(target=router_handler, args=(UDP_addr1, completed))
  th2=Thread(target=router_handler, args=(UDP_addr2, completed))
  th1.start()
  th2.start()
  try:
    while True:
      # Wait barrier
      completed.wait()
      reconstructAndSave()
      seq_num = 0
      # Reset barrier
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