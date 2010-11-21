(ns chinese.multicast
  (:use [chinese.protocols])
  (:import [java.util Date UUID]
           [java.net InetAddress MulticastSocket DatagramPacket]
           [org.apache.commons.codec.binary Base64]
           [java.io ByteArrayOutputStream ObjectOutputStream]))

(defn uuidbytes []
  (let [uuid (UUID/randomUUID)]
    (with-open [baos (ByteArrayOutputStream.)
                oos (ObjectOutputStream. baos)]
      (.writeLong oos (.getMostSignificantBits uuid))
      (.writeLong oos (.getLeastSignificantBits uuid))
      (.flush oos)
      (.flush baos)
      (.toByteArray baos))))

(defn generate-id []
  (Base64/encodeBase64URLSafeString (uuidbytes)))

(defn process-args [m]
  (mapcat (fn [[flag value]] [(format "--%s" (name flag)) value]) m))

(defn growl [m]
  (-> (Runtime/getRuntime)
      (.exec
       (into-array
        String
        (cons "/usr/local/bin/growlnotify" (process-args m))))
      (doto .waitFor)))

(defrecord Multicast [group socket pid state]
  Election
  (broadcast [el bytes]
    (try
      (.send socket
             (DatagramPacket.
              bytes (count bytes) group 6789))
      (catch Exception e
        (.printStackTrace e))))
  (receive [el]
    (try
      (let [bytes (byte-array 100)
            packet (DatagramPacket. bytes (count bytes))]
        (.receive socket packet)
        bytes)
      (catch Exception e
        (.printStackTrace e))))
  (master-elected [el id]
    (future
      (log el (str pid " says master is " id))
      (when (= id pid)
        (growl {:title pid
                :message "I am the master"}))))
  (id [_] pid)
  (election-interval [_] 10)
  (continue? [el]
    (not (= 1 (rand-int 50))))
  (handle-exception [_ exception]
    (locking #'println
      (.printStackTrace exception)))
  (log [_ string]
    (locking #'println
      (println (str (Date.) pid ">") string))))

(defn mcast []
  (let [group (InetAddress/getByName "228.5.6.7")
        s (doto (MulticastSocket. 6789)
            (.joinGroup group))
        id (generate-id)]
    (Multicast. group s id (atom {:master? false
                                  :count 0}))))
