# connectivity component

This component provides communication between things and other components.

for load balancing, only the messages get service that hash of their id`s is in range
(messages id from the same agent is the same)

also for future purpose, insert message detail to the database

and also to debugs, insert events and errors to corresponding collection of database


# light weight hash function (Optimize sha1):

input: (Recommended be less than 8 bytes, to spend less time for calculating)

output: 10 hex_digit that is hash of input (5 bytes)

Test:

    1-average time per execute in 100 try is 0.000132 (input:8 bytes -- cpu: intel Core i5 2.60 GHz  -- RAM: 6G)

	         !!!!! this value is 0.000218, in sha1 hash function !!!!!

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

		
# terminal command to run this components:


python connectivity.py broker_ip min_hash max_hash mongodb_ip

things must publish message to components with topic:d2i1820/agent/stc/# and also receives message with subscribe  to "d2i1820/agent/stt/#"

components must publish message to things with topic:stt/# and also receives message with subscribe  to "stc/#"