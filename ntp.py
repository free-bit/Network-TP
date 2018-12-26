from socket import *
from time import *

# Find fractional part of time information provided by NTP response
def getFraction(binary_repr):
  fraction=0.0
  ctr=1
  for bit in binary_repr:
    if(bit=="1"):
     fraction+=1/2**ctr
    ctr+=1
  return fraction

# Calculate NTP offset of this system
def getNTPTime(host = "pool.ntp.org"):
    port = 123
    buf = 1024
    host_address = (host,port)

    # Subtract 70 years of difference
    diff = 2208988800

    # Connect to NTP server
    client=socket(AF_INET, SOCK_DGRAM)
    client.settimeout(1)
    delay=10000
    offset=0
    delay_offset_pair=(delay, offset)
    # Send requests until getting 8 replies
    i=0
    while i<8:
      req=('\x1b'+47*'\0').encode()
      client.sendto(req, host_address)
      # Save current time
      client_tx=time()
      # Wait response
      data=0
      # If sent packet is lost, resend it without incrementing i
      try:
        data=client.recv(buf)
      except timeout:
        continue
      # Save current time
      client_rx=time()
      # Parse response
      response=struct.unpack('!12I', data)
      # Calculate receive timestamp
      decimal = response[8]-diff
      binary_repr=format(response[9],"b")
      binary_repr="0"*(32-len(binary_repr))+binary_repr
      fraction = getFraction(binary_repr)
      server_rx=decimal+fraction
      # Calculate transmit timestamp
      decimal = response[10]-diff
      binary_repr=format(response[11],"b")
      binary_repr="0"*(32-len(binary_repr))+binary_repr
      fraction = getFraction(binary_repr)
      server_tx=decimal+fraction
      # Bytes corrupted, discard
      if(server_rx>=server_tx):
        continue
      # Calculate:
      # The delay between the client and the server
      # The offset of the client clock from the server clock
      # NTP uses the minimum of the last eight delay measurements. 
      # The selected offset is one measured at the lowest delay.
      delay=(client_rx-client_tx)-(server_tx-server_rx)
      if(delay<delay_offset_pair[0]):
        offset=(server_rx-client_tx+server_tx-client_rx)/2
        delay_offset_pair=(delay, offset)
      # Send next request
      i+=1
    client.close()
    return offset