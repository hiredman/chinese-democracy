(ns chinese.protocols)

(defprotocol Election
  (broadcast [el bytes] "takes a byte array and broadcasts it")
  (receive [el] "recieves a byte array that was broadcast")
  (chairman-elected [el id] "called when a master is elected")
  (id [el] "id of this node, must a unique string, less then 90 bytes")
  (election-interval [el] "how long an election lasts in seconds")
  (continue? [el])
  (handle-exception [el exception])
  (log [el string]))
