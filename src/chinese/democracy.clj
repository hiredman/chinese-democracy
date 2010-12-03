(ns chinese.democracy
  (:use [chinese.protocols]
        [chinese.multicast :only [mcast]])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit])
  (:gen-class))

(defn serialize [x]
  (.getBytes (pr-str x)))

(defn deserialize [^bytes x]
  (when x
    (read-string (String. x "utf8"))))

(defn timeout [process opts]
  (if (= (id process) (:chairman opts))
    (* (election-interval process) 0.5)
    (election-interval process)))

(defn msg [^LinkedBlockingQueue inbox ^long timeout]
  (.poll inbox timeout TimeUnit/SECONDS))

(defn gt [a b]
  (pos? (compare a b)))

(defn run [inbox process]
  (let [election (serialize [:election (id process)])
        victory (serialize [:victory (id process)])]
    (letfn [(start [opts]
              (broadcast process election)
              #(set-chairman opts (:chairman opts)))
            (set-chairman [opts chairman]
              (chairman-elected process chairman)
              #(continue (assoc opts :chairman chairman)))
            (continue [opts]
              (log process (pr-str "chairman? " (:chairman? @(:state process))))
              (when (continue? process)
                #(wait opts (msg inbox (timeout process opts)))))
            (wait [opts [type node-id]]
              (cond
               (nil? node-id) #(victory opts)
               (= node-id (id process)) #(continue opts)
               (gt (id process) node-id) #(lesser-node node-id type opts)
               :else #(greater-node node-id type opts)))
            (victory [opts]
              (broadcast process victory)
              #(set-chairman opts (id process)))
            (lesser-node [node-id type opts]
              (if (and (or (= type :election)
                           (= type :victory))
                       (not (= (id process) node-id)))
                (do
                  (log process
                       (str "recieved "
                            type " from " node-id
                            " contesting!!!"))
                  #(start opts))
                #(continue opts)))
            (greater-node [node-id type opts]
              (if (or (gt node-id (:chairman opts))
                      (= node-id (:chairman opts)))
                #(set-chairman opts node-id)
                #(continue opts)))]
      (trampoline start {}))))

(defn ^Thread handle-incomming [^LinkedBlockingQueue inq p manager]
  (Thread.
   #(while true
      (try
        (let [some-bytes (receive p)
              msg (deserialize some-bytes)]
          (free manager some-bytes)
          (when (not= (id p) (second msg))
            (.put inq msg)))
        (catch Exception e
          (handle-exception p e))))))

;;I need to find a way to make this an agent
(defn ^Thread node [p]
  (let [inq (LinkedBlockingQueue.)
        manager (buffer-manager p)
        infut (doto (handle-incomming inq p manager)
                (.setName (str "InQ " (id p)))
                .start)]
    (doto (Thread.
           #(try
              (run inq p)
              (catch Exception e
                (handle-exception p e))
              (finally
               (try
                 (.stop infut)
                 (catch Exception e
                   (handle-exception p e))))))
      (.setName (id p)))))

(defn -main [& args]
  (while true
    (doto (node (mcast))
      .start
      .join)))
