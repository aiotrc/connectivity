"""
light weight hash function (Optimize sha1):

input: (Recommended be less than 8 bytes, to spend less time for calculating)
output: 10 hex_digit that is hash of input (5 bytes)

Test:
    1-average time per execute in 100 try is 0.000132 s (input:8 bytes -- cpu: intel Core i5 2.60 GHz  -- RAM: 6G)
         !!!!! this value is 0.000218 s, in sha1 hash function !!!!!

    2- outputs have Uniform distribution on domain
        for example: in 16000 execute (inputs:integer of 1 to 16000), first digit of output, is like below:
            For 1005 time is 0
            For 975 time is 1
            For 976 time is 2
            For 1013 time is 3
            For 926 time is 4
            For 974 time is 5
            For 1027 time is 6
            For 1128 time is 7
            For 1005 time is 8
            For 1060 time is 9
            For 1023 time is a
            For 968 time is b
            For 999 time is c
            For 1000 time is d
            For 947 time is e
            For 974 time is f

        and results of other digits, is similar above

"""

from __future__ import print_function
import struct
import io


__author__ = "Mehran Hosseinzade"
__email__ = "M.hosseinzade@eng.ui.ac.ir"

try:
    range = xrange    # xrange is faster
except NameError:
    pass


def _left_rotate(n, b):
    # Left rotate a 8-bit integer n by b bits.
    return ((n << b) | (n >> (8 - b))) & 0xff


def _process_chunk(chunk, h0, h1, h2, h3, h4):
    # Process a chunk of data and return the new digest variables.
    assert len(chunk) == 16

    w = [0] * 80

    # Break chunk into sixteen byte big-endian w[i]
    for i in range(16):
        w[i] = struct.unpack(b'>b', chunk[i])[0]
        # <	little-endian	standard
        #  >	big-endian	standard
        # b: byte

    # Extend the sixteen byte into eighty byte
    for i in range(16, 80):
        w[i] = _left_rotate(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1)  # a ^ b	xor(a, b)

    # Initialize hash value for this chunk
    a = h0
    b = h1
    c = h2
    d = h3
    e = h4

    for i in range(80):
        if 0 <= i <= 19:
            f = d ^ (b & (c ^ d))
            k = 0x5A
        elif 20 <= i <= 39:
            f = b ^ c ^ d
            k = 0x6E
        elif 40 <= i <= 59:
            f = (b & c) | (b & d) | (c & d)
            k = 0x8F
        elif 60 <= i <= 79:
            f = b ^ c ^ d
            k = 0xCA

        a, b, c, d, e = ((_left_rotate(a, 1) + f + e + k + w[i]) & 0xff,
                         a, _left_rotate(b, 5), c, d)

    # Add this chunk's hash to result so far
    h0 = (h0 + a) & 0xff
    h1 = (h1 + b) & 0xff
    h2 = (h2 + c) & 0xff
    h3 = (h3 + d) & 0xff
    h4 = (h4 + e) & 0xff

    return h0, h1, h2, h3, h4


class Hash(object):
    digest_size = 40  # bit (5 byte)
    block_size = 16

    def __init__(self):
        # Initial digest variables
        self._h = (
            0x67,
            0xEF,
            0x98,
            0x10,
            0xC3,
        )

        # bytes object with 0 <= len < 16 used to store the end of the message
        # if the message length is not congruent to 16
        self._unprocessed = b''
        # Length in bytes of all data that has been processed so far
        self._message_byte_length = 0

    def update(self, arg):
        """Update the current digest.
        Arguments:
            arg: bytes, bytearray, or BytesIO object to read from.
        """
        if isinstance(arg, (bytes, bytearray)):
            arg = io.BytesIO(arg)

        """ bytes is an immutable type, every time we append more bytes to buffer Python has to allocate
            the variable as the concatenation of buffer and the return value of read_from_socket.
            Concatenation is slow in Python and it shows when you`re processing high volume of data.
        """
        chunk = self._unprocessed + arg.read(16 - len(self._unprocessed))

        # Read the rest of the data, 16 bytes at a time
        while len(chunk) == 16:
            self._h = _process_chunk(chunk, *self._h)   # * collects all the positional arguments in a tuple
            self._message_byte_length += 16
            chunk = arg.read(16)

        self._unprocessed = chunk
        return self

    def hexdigest(self):
        """Produce the final hash value (big-endian) as a hex string"""
        return '%02x%02x%02x%02x%02x' % self._produce_digest()
        # 8 says that you want to show 8 digits
        # 0 that you want to prefix with 0's instead of just blank spaces
        # x that you want to print in lower-case hexadecimal.

    def _produce_digest(self):
        """Return finalized digest variables."""
        # Pre-processing:
        message = self._unprocessed
        message_byte_length = self._message_byte_length + len(message)

        # append 0 <= k < 64 bits '0'
        message += b'\x00' * ((8 - (message_byte_length) % 16) % 16)

        # append length of message
        message_bit_length = message_byte_length * 8
        message += struct.pack(b'>Q', message_bit_length)  # Q: long 8 byte

        # Process the final chunk
        # At this point, the length of the message is either 16 or 32 bytes.
        h = _process_chunk(message[:16], *self._h)
        if len(message) == 16:
            return h
        return _process_chunk(message[16:], *h)


def LWhash(data):
    """LWhash Hashing Function
    Arguments:
        data: A bytes or BytesIO object containing the input message to hash.
    Returns:
        hash digest of the input message.
    """
    return Hash().update(str(data)).hexdigest()
