RN_STATUS = (RN_UNKNOWN,
             RN_BOOTING,
             RN_PAUSED,
             RN_IDLE,
             RN_ASSIGNED,
             RN_WORKING,
             RN_FINISHING) = range(7)

RN_STATUS_NAMES = ('Unknown', 'Booting', 'Paused', 'Idle', 'Assigned', 'Working', 'Finishing')

RN_STATUS_SHORT_NAMES = ("U", "B", "P", "I", "A", "W", "F")

# 0 --> RN_UNKNOWN
# 1 --> RN_BOOTING
# 2 --> RN_PAUSED
# 3 --> RN_IDLE
# 4 --> RN_ASSIGNED
# 5 --> RN_WORKING
# 6 --> RN_FINISHING
