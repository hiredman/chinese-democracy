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

;; bully algorithm state machine
(defn run [inbox process]
  ;; allocate and serialize the two possible messages to send once
  ;; (cuts down on saw-tooth allocation profile)
  (let [election-msg (serialize [:election (id process)])
        victory-msg (serialize [:victory (id process)])]
    (letfn [(start [opts]
              ;; start by annoucing to anyone listening that you want
              ;; to be chairman
              (broadcast process election-msg)
              ;; until you decide you've won, or some one else wins,
              ;; set the chairman to be the chairman you started with (nil)
              #(set-chairman opts (:chairman opts)))
            (set-chairman [opts chairman]
              (log process (format "master elected: %s" chairman))
              (chairman-elected process chairman)
              #(continue (assoc opts :chairman chairman)))
            (continue [opts]
              ;; this is a kill switch for stopping the statemachine
              ;; if required
              (when (continue? process)
                #(wait opts (msg inbox (timeout process opts)))))
            (wait [opts [type node-id]]
              ;; wait for incoming messages and respond appropriately
              (cond
               (= :fault type) #(fault opts)
               (nil? node-id) #(victory opts)
               (= node-id (id process)) #(continue opts)
               (gt (id process) node-id) #(lesser-node node-id type opts)
               :else #(greater-node node-id type opts)))
            (fault [opts]
              (Thread/sleep (* 1000 (timeout process opts) 2))
              #(wait opts (msg inbox (timeout process opts))))
            (victory [opts]
              ;; broadcast your victory to others and set yourself to
              ;; be the chairman
              (log process (format "I won the election"))
              (broadcast process victory-msg)
              #(set-chairman opts (id process)))
            (lesser-node [node-id type opts]
              ;;TODO: rip out this conditional
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
              ;; a greater node always gets to be chairman
              (if (or (gt node-id (:chairman opts))
                      (= node-id (:chairman opts)))
                #(set-chairman opts node-id)
                #(continue opts)))]
      (trampoline start {}))))

;; TODO: remove calls to Thread#stop() use some kind of sentinal/kill switch

(defn ^Thread handle-incoming [^LinkedBlockingQueue inq p manager]
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
        infut (doto (handle-incoming inq p manager)
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
