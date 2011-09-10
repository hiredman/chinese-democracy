(ns chinese.democracy
  (:use [chinese.protocols]
        [chinese.multicast :only [mcast]])
  (:import (java.util.concurrent LinkedBlockingQueue TimeUnit))
  (:gen-class))

(defn serialize [x]
  (.getBytes (pr-str x)))

(defn deserialize [^bytes x]
  (when x
    (read-string (String. x "utf8"))))

(defn subordinate? [opts]
  (= :subordinate (:state opts)))

(defn electing? [opts]
  (= :electing (:state opts)))

(defn chairman? [opts]
  (= :chairman (:state opts)))

(defn timeout [process opts]
  (if (or (electing? opts) (chairman? opts))
    (* (election-interval process) 0.5)
    (election-interval process)))

(defn msg [^LinkedBlockingQueue inbox ^long timeout]
  (.poll inbox timeout TimeUnit/SECONDS))

(defn gt [a b]
  (pos? (compare a b)))

(defn timestamp []
  (System/currentTimeMillis))

(defn timeout? [process ts]
  (> (timestamp)
     (+ ts
        (* (election-interval process) 1000))))

(defn reset?
  "If a node gets too busy doing other stuff, and hasn't been communicating
  with the other nodes, then they will consider it failed, so it should reset
  itself."
  [process opts]
  (and (not (subordinate? opts))
       (timeout? process (:last-activity opts))))

(defn call-for-election?
  "If a node was subordinated by a greater node, but has not heard from it
  since, then it's time to call for an election."
  [process opts]
  (and (subordinate? opts)
       (timeout? process (:last-bullied opts))))

(defn declare-victory?
  "If a node started an election, and has not been bullied for the election's
  duration, then declare victory."
  [process opts]
  (and (electing? opts)
       (timeout? process (:election-start opts))))

;; bully algorithm state machine
(defn run [inbox process]
  ;; allocate and serialize the two possible messages to send once
  ;; (cuts down on saw-tooth allocation profile)
  (let [election-msg (serialize [:election (id process)])
        victory-msg (serialize [:victory (id process)])
        submission-msg (serialize [:submission (id process)])]
    (letfn [(reset [opts]
              (chairman-elected process nil)
              (assoc opts
                :state :subordinate
                :allegiance (id process)
                :last-bullied (timestamp)
                :last-activity (timestamp)))
            (election [opts]
              (chairman-elected process nil)
              (log process "I would like to be chairman")
              ;; start by annoucing to anyone listening that you want
              ;; to be chairman
              (broadcast process election-msg)
              ;; until you decide you've won, or some one else wins,
              ;; set the chairman to be the chairman you started with (nil)
              #(continue (assoc opts
                           :state :electing
                           :allegiance (id process)
                           :last-activity (timestamp)
                           :election-start (timestamp))))
            (continue [opts]
              ;; this is a kill switch for stopping the statemachine
              ;; if required
              (when (continue? process)
                (wait opts (msg inbox (timeout process opts)))))
            (wait [opts [type node-id]]
              ;; wait for incoming messages and respond appropriately
              (let [opts (if (reset? process opts)
                           (reset opts)
                           opts)]
                (cond
                 (= :fault type) #(wait opts (msg inbox (timeout process opts)))
                 (nil? node-id) (if (declare-victory? process opts)
                                  (victory opts)
                                  (if (call-for-election? process opts)
                                    (election opts)
                                    (submission opts)))
                 (= node-id (id process)) #(continue opts)
                 (gt node-id (id process)) (greater-node node-id type opts)
                 :else (lesser-node node-id type opts))))
            (submission [opts]
              (broadcast process submission-msg)
              #(continue (assoc opts :last-activity (timestamp))))
            (victory [opts]
              ;; broadcast your victory to others and set yourself to
              ;; be the chairman
              (log process (format "I won the \"election\""))
              (broadcast process victory-msg)
              (chairman-elected process (id process))
              #(continue (assoc opts
                           :state :chairman
                           :last-activity (timestamp))))
            (lesser-node [node-id type opts]
              (if (call-for-election? process opts)
                (election opts)
                (if (and (not (subordinate? opts))
                         (or (= :victory type)
                             (= :submission type)))
                  (do
                    (log process (format "%s does not know his place"
                                         node-id))
                    (election opts))
                  #(continue opts))))
            (greater-node [node-id type opts]
              (when-not (subordinate? opts)
                (log process "I am bullied"))
              ;; a greater node always gets to be chairman
              #(continue (assoc opts
                           :state :subordinate
                           :allegiance (if (gt node-id (:allegiance opts))
                                         (do (chairman-elected process node-id)
                                             node-id)
                                         (:allegiance opts))
                           :last-bullied (timestamp))))]
      (trampoline continue (reset {})))))

(defn ^Thread handle-incoming [^LinkedBlockingQueue inq p manager]
  (Thread.
   #(while (continue? p)
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
                (handle-exception p e))))
      (.setName (id p)))))

(defn -main [& args]
  (while true
    (doto (node (mcast))
      .start
      .join)))
