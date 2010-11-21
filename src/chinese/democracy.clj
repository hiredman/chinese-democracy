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
            #(set-master opts (:master opts)))
          (set-master [opts master]
            (when (not= (:master opts) master)
              (master-elected process master))
            #(continue (assoc opts :master master)))
          (continue [opts]
            (when (continue? process)
              #(wait opts (msg inbox (timeout process opts)))))
          (wait [opts [type node-id]]
            (log process (str :wait " " (:master opts)))
            (cond
             (nil? node-id) #(victory opts)
             (= node-id (id process)) #(continue opts)
             (gt (id process) node-id) #(lesser-node node-id type opts)
             :else #(greater-node node-id type opts)))
          (victory [opts]
            (broadcast process (serialize [:election (id process)]))
            #(set-master opts (id process)))
          (lesser-node [node-id type opts]
            (if (and (or (= type :election)
                         (= type :victory))
                     (not (= (id process) node-id)))
              #(start opts)
              #(continue opts)))
          (greater-node [node-id type opts]
            (if (or (gt node-id (:master opts))
                    (= node-id (:master opts)))
              #(set-master opts node-id)
              #(continue opts)))]
    (trampoline start {})))

(defn handle-incomming [inq p]
  (future
    (while true
      (try
        (.put inq (receive p))
        (catch Exception e
          (handle-exception p e))))))

;;I need to find a way to make this an agent
(defn node [p]
  (let [inq (LinkedBlockingQueue.)
        infut (handle-incomming inq p)]
    (future
      (try
        (run inq p)
        (catch Exception e
          (handle-exception p e))
        (finally
         (try
           (future-cancel infut)
           (catch Exception e
             (handle-exception p e))))))))

(defn -main [& args]
  (while true
    @(node (mcast))))
