(ns chinese.democracy
  (:use [chinese.protocols]
        [chinese.multicast :only [mcast]])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit])
  (:gen-class))

(defn serialize [x]
  (.getBytes (pr-str x)))

(defn deserialize [x]
  (when x
    (read-string (String. x "utf8"))))

(defn timeout [process opts]
  (if (= (id process) (:master opts))
    (* (election-interval process) 0.9)
    (election-interval process)))

(defn msg [inbox timeout]
  (deserialize
   (.poll inbox timeout TimeUnit/SECONDS)))

(defn gt [a b]
  (pos? (compare a b)))

(defn run [inbox process]
  (letfn [(start [opts]
            (broadcast process (serialize [:election (id process)]))
            #(set-master opts (id process)))
          (set-master [opts master]
            (master-elected process master)
            #(continue (assoc opts :master (id process))))
          (continue [opts]
            (when (continue? process)
              #(wait opts)))
          (wait [opts]
            (let [[type node-id] (msg inbox (timeout process opts))]
              (cond
               (nil? node-id) #(victory opts)
               (= node-id (id process)) #(continue opts)
               (gt (id process) node-id) #(lesser-node node-id type opts)
               :else #(greater-node node-id type opts))))
          (victory [opts]
            (broadcast process (serialize [:victory (id process)]))
            #(set-master opts (id process)))
          (lesser-node [node-id type opts]
            (if (or (= type :election)
                    (= type :victory))
              #(start opts)
              #(continue opts)))
          (greater-node [node-id type opts]
            (if (gt node-id (:master opts))
              #(continue (assoc opts :master node-id))
              #(continue opts)))]
    (trampoline start {})))

(defn handle-incomming [inq p]
  (future
    (while true
      (.put inq (receive p)))))

;;I need to find a way to make this an agent
(defn node [p]
  (let [inq (LinkedBlockingQueue.)
        infut (handle-incomming inq p)]
    (future
      (try
        (run inq p)
        (catch Exception e
          (.printStackTrace e))
        (finally
         (try
           (future-cancel infut)
           (catch Exception e
             (.printStackTrace e))))))))

(defn -main [& args]
  (while true
    @(node (mcast))))
