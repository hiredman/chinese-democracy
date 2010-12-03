(ns chinese.multicast
  (:use [chinese.protocols])
  (:import [java.util Date UUID]
           [java.net InetAddress MulticastSocket DatagramPacket]
           [org.apache.commons.codec.binary Base64]
           [java.io ByteArrayOutputStream ObjectOutputStream]
           [java.lang.management ManagementFactory]))

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
  (format "%s-%s-%s"
          (rand-int 1024)
          (.getHostName (InetAddress/getLocalHost))
          (-> (ManagementFactory/getRuntimeMXBean)
              .getName (.split "@") first)))

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
  (broadcast [el some-bytes]
    (try
      (.send ^MulticastSocket socket
             (DatagramPacket.
              ^bytes some-bytes (count some-bytes)
              ^InetAddress group 6789))
      (catch Exception e
        (.printStackTrace e))))
  (receive [el]
    (try
      (let [bytes (byte-array 100)
            packet (DatagramPacket. bytes (count bytes))]
        (.receive ^MulticastSocket socket packet)
        bytes)
      (catch Exception e
        (.printStackTrace e))))
  (master-elected [el id]
    (swap! state update-in [:master?] (constantly (= id pid))))
  (id [_] pid)
  (election-interval [_] 120)
  (continue? [el]
    (pos? (rand-int 50)))
  (handle-exception [_ exception]
    (locking #'println
      (.printStackTrace ^Exception exception)))
  (log [_ string]
    (locking #'println
      (println (format "%s %s> %s" (Date.) pid string)))))

(defn mcast []
  (let [group (InetAddress/getByName "228.5.6.7")
        s (doto (MulticastSocket. 6789)
            (.joinGroup group))
        id (generate-id)]
    (Multicast. group s id (atom {:master? false
                                  :count 0}))))
