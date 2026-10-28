

sqlite-express (encrypt/compress/rockdb) + cr-sqlite + murmur-replication (QUIC/SWIM)
    1x at 183,693/s  and 100x at 372,226/s

                    VS

Sqlite-express (encrypt/compress/rockdb) + session-ext + murmur-engine-spool (rockdb for data + spool file for replication) + murmur-replication

(murmur-replication would have to be changed to adapt to murmur-engine-spool instead of cr-sqlite)


