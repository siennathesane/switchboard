(ns jepsen.switchboard.checkers
  "Custom checkers for switchboard's ordering guarantees.

  Switchboard assigns every queue to a single shard raft group, so all
  enqueues to a queue (from any node) serialize in that group's log, and all
  hand-outs serialize through the same log. That gives us three testable
  properties:

  * FIFO     - within one queue, first deliveries of messages from a single
               publisher arrive in publish order; acknowledged publishes are
               never lost; messages delivered twice must have been
               redelivered (at-least-once, no phantom resurrections).
  * Fanout   - the same publish routed to K queues on K shard groups commits
               in K independent logs. If the cluster provides a global total
               order, every queue's confirmed-message stream is identical.
               Divergence means per-queue orders disagree: the system is not
               totally ordered across queues. This checker measures both.
  * Register - queue existence (declare/delete/passive read) is replicated
               through the meta raft group; every synchronous reply is
               linearizable. Checked with Knossos via the ExReg model."
  (:require [clojure [set :as set]]
            [jepsen [checker :as checker]
                    [history :as h]]
            [knossos [model :as model]])
  (:import (knossos.model Model)))

(defn- pairs
  "A set of [pid seq] pairs from a map of pid -> (sorted set of seqs)."
  [m]
  (set (for [[pid seqs] m
             s seqs]
         [pid s])))

(defn- delivery-fold
  "Folds a history into

    {:confirmed  {pid #{seqs}}  publishes confirmed by the broker
     :attempted  {pid #{seqs}}  all publishes (ok confirmed, ok unconfirmed
                                is NOT attempted-confirmed but attempted,
                                and :info outcomes)
     :stream     [...]          deliveries in observation order
     :depths     {queue depth}  last observed depth per queue}

  `keep-queue?` controls whether get ops record their queue in the stream
  (multi-queue tests) or not (single-queue tests)."
  [keep-queue? history]
  (reduce (fn [a op]
            (case (:f op)
              :publish
              (let [{:keys [pid seq confirmed]} (:value op)]
                (cond-> (cond-> a
                          (and pid seq)
                          (update-in [:attempted pid] (fnil conj (sorted-set)) seq))
                  (and (= :ok (:type op)) confirmed pid seq)
                  (update-in [:confirmed pid] (fnil conj (sorted-set)) seq)))

              :get
              (if (h/ok? op)
                (let [{:keys [queue msg redelivered depth]} (:value op)]
                  (cond-> a
                    msg  (update :stream conj
                                 {:queue (when keep-queue? queue)
                                  :msg   msg
                                  :redelivered redelivered
                                  :index  (:index op)})
                    (and (some? depth) queue) (assoc-in [:depths queue] depth)))
                a)

              :depth
              (if (h/ok? op)
                (let [{:keys [queue depth]} (:value op)]
                  (cond-> a
                    (and queue (some? depth))
                    (assoc-in [:depths queue] (if (neg? depth) 0 depth))))
                a)

              ;; nemesis ops, :declare/:delete/:exists, logs, ...
              a))
          {:confirmed {} :attempted {} :stream [] :depths {}}
          history))

(defn- order-scan
  "Scans a delivery stream (in observation order). Returns
  {:first-seen  {[pid seq] index}
   :last-seq    {pid seq}            (last first-occurrence seq per pid)
   :order-anomalies [...]
   :dups        [...]                (repeat deliveries)
   :unauthorized-dups [...]}         (repeat deliveries NOT flagged redelivered)"
  [stream]
  (reduce (fn [a {:keys [msg redelivered index]}]
            (if (contains? (:first-seen a) msg)
              ;; A repeat delivery. Legal only if the broker flagged it
              ;; redelivered.
              (cond-> (update a :dups conj {:msg msg, :index index})
                (not redelivered)
                (update :unauthorized-dups conj
                        {:msg msg, :redelivered redelivered, :index index}))
              ;; First occurrence: check per-publisher order.
              (let [[pid seq] msg
                    last-seq (get-in a [:last-seq pid])]
                (cond-> (-> a
                            (assoc-in [:first-seen msg] index)
                            (assoc-in [:last-seq pid] seq))
                  (and last-seq (< seq last-seq))
                  (update :order-anomalies conj
                          {:pid pid, :delivered seq, :after last-seq
                           :index index})))))
          {:first-seen {} :last-seq {}
           :order-anomalies [] :dups [] :unauthorized-dups []}
          stream))

(defn- verdict
  "Combines hard violations with drain state into a final result. `lost`
  messages with an incomplete drain downgrade validity to :unknown."
  [hard-violations lost drain-complete? extra]
  (merge {:valid?          (if (seq hard-violations)
                             false
                             (if (or (empty? lost) drain-complete?)
                               true
                               :unknown))
          :lost-count      (count lost)
          :drain-complete? drain-complete?}
         (when (and (empty? hard-violations) (seq lost) (not drain-complete?))
           {:caveat "Queue(s) not fully drained; 'lost' messages may still be in flight"})
         extra))

;; Public checkers

(defn fifo-checker
  "Single-queue FIFO, loss, and redelivery discipline. Assumes all :get ops
  come from a single worker at a time (one poller), so the delivery stream
  order is authoritative. Requires a final :depth op to confirm the queue
  drained."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [{:keys [confirmed attempted stream depths]} (delivery-fold false history)
            scan  (order-scan stream)
            conf  (pairs confirmed)
            att   (pairs attempted)
            d1st  (set (keys (:first-seen scan)))
            lost  (set/difference conf d1st)
            unexpected (set/difference d1st att)
            last-depth (when (seq depths) (reduce max (vals depths)))
            drain-complete? (and (some? last-depth) (zero? last-depth))
            hard  (concat (:order-anomalies scan)
                          (:unauthorized-dups scan)
                          unexpected)]
        (verdict hard lost drain-complete?
                 {:confirmed-count      (count conf)
                  :attempted-count      (count att)
                  :delivered-count      (count stream)
                  :distinct-delivered   (count d1st)
                  :duplicate-deliveries (count (:dups scan))
                  :redelivered-first    (count (filter :redelivered stream))
                  :order-anomalies      (take 32 (:order-anomalies scan))
                  :order-anomaly-count  (count (:order-anomalies scan))
                  :unauthorized-dups    (take 32 (:unauthorized-dups scan))
                  :lost                 (take 32 (sort lost))
                  :unexpected           (take 32 unexpected)
                  :final-depth          last-depth})))))

(defn fanout-checker
  "K durable queues bound to one fanout exchange. Every confirmed publish
  must eventually appear in every queue, and (this is the total-order
  property) all queues must agree on the order of confirmed messages.

  Streams are per-poller-thread observation orders, which is exactly the
  notion of 'what a consumer sees'. Requires one poller per queue and a
  final :depth op per queue."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [{:keys [confirmed attempted stream depths]} (delivery-fold true history)
            conf  (pairs confirmed)
            att   (pairs attempted)

            ;; Per-queue delivery streams in observation order.
            streams (->> stream
                         (group-by :queue)
                         (mapv (fn [[q msgs]]
                                 [q (mapv :msg msgs)]))
                         (into (sorted-map)))

            ;; Global scan across all streams for delivery-count bookkeeping.
            scan (order-scan stream)
            d1st (set (keys (:first-seen scan)))
            unexpected (set/difference d1st att)

            ;; Restrict each queue's stream to confirmed messages. Total
            ;; order means all queues agree on the RELATIVE ORDER of the
            ;; messages they co-deliver: a queue may legitimately skip a
            ;; message for a while (its own poller holds it after a lost
            ;; get response), so streams are compared as subsequences over
            ;; the co-delivered set rather than required to be identical.
            restricted (mapv (fn [[q msgs]]
                               [q (filterv conf msgs)])
                             streams)
            [q0 msgs0] (first restricted)
            divergences (->> (drop 1 restricted)
                             (keep (fn [[q msgs]]
                                     (let [common (into #{} msgs0)
                                           theirs (filterv (fn [m] (contains? common m)) msgs)
                                           common2 (into #{} msgs)
                                           mine (filterv (fn [m] (contains? common2 m)) msgs0)]
                                       (->> (map vector (range) mine theirs)
                                            (filter (fn [[i a b]] (not= a b)))
                                            (map (fn [[i a b]]
                                                   {:queue q, :vs q0, :at i,
                                                    :expected a, :got b}))
                                            seq
                                            first))))
                             (remove nil?))

            per-q (mapv (fn [[q msgs]]
                          [q (order-scan (->> stream
                                              (filter (fn [d] (= q (:queue d))))
                                              (mapv #(dissoc % :queue))))])
                        streams)
            q-anomalies (mapcat (fn [[q s]] (map #(assoc % :queue q)
                                                 (:order-anomalies s)))
                                per-q)
            q-unauth-dups (mapcat (fn [[q s]] (map #(assoc % :queue q)
                                                   (:unauthorized-dups s)))
                                  per-q)

            ;; Confirmed messages that never showed up in a given queue.
            missing (mapv (fn [[q msgs]]
                            [q (set/difference conf (set msgs))])
                          streams)
            lost (reduce set/union #{} (map second missing))

            ;; Drain bookkeeping: each queue's last depth observation.
            last-depths (into {} depths)
            drain-complete? (and (>= (count last-depths) (count streams))
                                 (every? #(or (zero? %) (neg? %))
                                         (vals last-depths)))
            hard (concat divergences q-anomalies q-unauth-dups unexpected)]

        (verdict hard lost drain-complete?
                 {:confirmed-count        (count conf)
                  :attempted-count        (count att)
                  :delivered-count        (count stream)
                  :stream-sizes           (mapv (fn [[q m]] [q (count m)]) streams)
                  :confirmed-in-stream    (mapv (fn [[q m]] [q (count (filterv conf m))])
                                                restricted)
                  :order-divergences      (take 32 divergences)
                  :order-divergence-count (count divergences)
                  :missing-per-queue      (mapv (fn [[q s]] [q (count s)]) missing)
                  :order-anomalies        (take 32 q-anomalies)
                  :order-anomaly-count    (count q-anomalies)
                  :unauthorized-dups      (take 32 q-unauth-dups)
                  :lost                   (take 32 (sort lost))
                  :unexpected             (take 32 unexpected)
                  :final-depths           last-depths})))))

;; Linearizability model for the topology register.

(defrecord ExReg [present]
  Model
  (step [m op]
    (case (:f op)
      :declare
      (ExReg. true)

      :delete
      (let [claimed (get-in op [:value :deleted])]
        (if (= claimed present)
          (ExReg. false)
          (model/inconsistent
            (str "delete returned deleted=" claimed
                 " but queue was " (if present "present" "absent")))))

      :exists
      (let [claimed (get-in op [:value :present])]
        (if (= claimed present)
          m
          (model/inconsistent
            (str "passive declare returned present=" claimed
                 " but queue was " (if present "present" "absent")))))

      m)))

(defn ex-reg
  "The register starts absent."
  []
  (ExReg. false))
