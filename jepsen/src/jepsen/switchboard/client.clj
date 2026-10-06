(ns jepsen.switchboard.client
  "Jepsen clients speaking AMQP 0-9-1 to switchboard via the RabbitMQ Java
  client.

  Every worker owns one connection (created lazily, abandoned on any
  error). Ops are role-agnostic: the generator decides which threads publish
  or poll; each client resolves queue/exchange names from its configuration.

  Publishes use publisher confirms and report

    {:pid \"node#uid\" :seq n :confirmed true|false}

  where :confirmed true means the broker quorum-committed the enqueue on
  every routed queue. Polls (basic.get) report

    {:msg [pid seq]|nil :redelivered b :depth n}

  and topology ops (:declare/:delete/:exists on a single register queue)
  report definitive results used by the linearizability checker."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]]
            [jepsen.switchboard.amqp :as amqp])
  (:import (com.rabbitmq.client Connection Channel)))

(def confirm-timeout-ms
  "How long to wait for a publisher confirm before declaring the outcome
  indeterminate. Switchboard's own write budget is 5s; we allow one second
  of margin and let the checker treat the op as :info."
  6000)

(def readiness-roundtrip
  "Max seconds for the cluster readiness gate in setup!."
  240)

;; Lifecycle

(defn- mark-dead!
  "Any pending confirms become indeterminate; the connection is discarded."
  [c]
  (amqp/fail-pending! (:pending c) :dead)
  (amqp/close-quietly @(:conn c))
  (reset! (:conn c) nil)
  (reset! (:ch c) nil))

(defn- ensure-conn!
  ^Connection [c]
  (or (when (amqp/open? @(:conn c)) @(:conn c))
      (do (amqp/close-quietly @(:conn c))
          (let [conn (amqp/connect (:node c) (:conn-opts c))]
            (reset! (:conn c) conn)
            conn))))

(defn- ensure-ch!
  "Returns an open channel; a confirm-mode channel when `confirm?`. Channel
  errors close the channel, so we recreate whenever it's not open. The
  pending-confirm atom is shared for the life of the client; each new
  confirm channel bumps :chan-gen (confirm sequences restart at 1 per
  channel, so the pid embeds the generation to stay unique)."
  ^Channel [c confirm?]
  (or (when (amqp/open? @(:ch c)) @(:ch c))
      (let [conn (ensure-conn! c)
            ch   (if confirm?
                   (do (swap! (:chan-gen c) inc)
                       (amqp/confirm-channel! conn (:pending c)))
                   (amqp/channel conn))]
        (reset! (:ch c) ch)
        ch)))

;; Readiness

(defn- roundtrip!
  "A full publish+confirm+consume roundtrip through this client's node,
  against a scratch queue. Throws if the cluster isn't serving yet."
  [c]
  (let [q    (str "je.ready." (:node c))
        ch   (ensure-ch! c true)]
    (try (amqp/delete-queue! ch q) (catch Exception _))
    ;; ensure-ch! above may have returned a channel that just died on the
    ;; delete; recreate when needed.
    (let [ch (ensure-ch! c true)]
      (amqp/declare-queue! ch q)
      (let [res (amqp/publish-confirm! ch (:pending c) "" q
                                       (amqp/encode-msg "setup" 0)
                                       5000)]
        (when-not (= :ack res)
          (throw (ex-info "readiness publish not confirmed" {:result res}))))
      (if-let [m (amqp/get-message ch q)]
        (do (amqp/ack ch (:tag m))
            (amqp/delete-queue! ch q))
        (throw (ex-info "readiness get came up empty" {}))))))

(defn- await-cluster!
  "Retries (f) every second until it stops throwing, or `max-s` seconds
  elapse (then the last exception propagates)."
  [max-s f]
  (let [deadline (+ (System/nanoTime) (* (long max-s) 1000000000))]
    (loop []
      (let [res (try {:ok (f)}
                     (catch Exception e
                       (if (neg? (compare (System/nanoTime) deadline))
                         (do (info "waiting for switchboard cluster:"
                                   (.getName (class e)) (.getMessage e))
                             (Thread/sleep 1000)
                             ::retry)
                         (throw e))))]
        (cond
          (= ::retry res) (recur)
          (contains? res :ok) (:ok res))))))

(defn- declare-topology-once!
  [c]
  (let [topology (:topology c)
        ch       (ensure-ch! c false)]
    (doseq [q (:queues topology)]
      (amqp/declare-queue! ch q))
    (when-let [x (:exchange topology)]
      (amqp/declare-fanout! ch x)
      (doseq [q (:queues topology)]
        (amqp/bind! ch q x)))
    ;; Warmup: one confirmed publish + get on a dedicated queue proves
    ;; routing is fresh on this node for newly-declared queues, without
    ;; touching test queues.
    (when-let [w (:warmup topology)]
      (amqp/declare-queue! ch w)
      (let [res (amqp/publish-confirm! ch (:pending c) "" w
                                      (amqp/encode-msg "setup" 0)
                                      5000)]
        (when-not (= :ack res)
          (throw (ex-info "warmup publish not confirmed" {:result res}))))
      (if-let [m (amqp/get-message ch w)]
        (amqp/ack ch (:tag m))
        (throw (ex-info "warmup get came up empty" {})))
      (try (amqp/delete-queue! ch w) (catch Exception _)))))

(defn- setup-topology!
  [c test]
  (let [{:keys [topology]} c]
    ;; Cluster readiness: retry the roundtrip until it works.
    (await-cluster! readiness-roundtrip #(roundtrip! c))
    ;; Test topology (idempotent declares). Concurrent declares of a fresh
    ;; queue can lose a race against the creator's shard-side CreateQueueData
    ;; and come back 404; that's benign, so retry a few times.
    (when topology
      (loop [attempts 6]
        (let [res (try {:ok (declare-topology-once! c)}
                       (catch Exception e
                         (when (pos? (dec attempts))
                           (info "topology declare raced; retrying:"
                                 (.getMessage e))
                           (Thread/sleep 500)
                           ::retry)))]
          (cond
            (= ::retry res) (recur (dec attempts))
            (contains? res :ok) (:ok res)
            :else (throw (ex-info "topology setup failed" {}))))))
    ;; The topology register starts absent.
    (when (:register c)
      (let [ch (ensure-ch! c false)]
        (try (amqp/delete-queue! ch (:register c)) (catch Exception _))))))

;; Ops

(defn- publish-op!
  [c op]
  (try
    (let [ch      (ensure-ch! c true)
          pid     (str (:node c) "#" (:uid c) "-" @(:chan-gen c))
          seq     (.getNextPublishSeqNo ^Channel ch)
          p       (promise)
          {:keys [exchange routing-key]} (:publish c)]
      (swap! (:pending c) assoc seq p)
      (amqp/publish! ch (or exchange "") (or routing-key "")
                     (amqp/encode-msg pid seq))
      (let [res (deref p confirm-timeout-ms :timeout)]
        (swap! (:pending c) dissoc seq)
        (case res
          :ack     (assoc op :type :ok
                          :value {:pid pid, :seq seq, :confirmed true})
          :nack    (assoc op :type :fail
                          :value {:pid pid, :seq seq, :confirmed false})
          (assoc op :type :info
                 :value {:pid pid, :seq seq, :confirmed false}))))
    (catch Exception e
      (mark-dead! c)
      (assoc op :type :info
             :value {:confirmed false, :error (amqp/error-info e)}))))

(def get-timeout-ms
  "basic.get has no client-side timeout in amqp-client: a reply lost to a
  blackhole partition would block the worker forever and keep the message
  held server-side. Bound it; on timeout we drop the whole connection,
  which makes the broker release the hold and the future throw."
  5000)

(defn- get-op!
  [c op]
  (try
    (let [ch (ensure-ch! c false)
          q  (:queue c)
          ;; ::pending only materializes on timeout; the connection kill
          ;; must NOT run eagerly (deref's default is a plain value).
          m  (let [fut (future (amqp/get-message ch q))
                   res (deref fut get-timeout-ms ::pending)]
               (if (= ::pending res)
                 (do (amqp/close-quietly @(:conn c)) :timeout)
                 res))]
      (cond
        (= :timeout m)
        ;; Indeterminate: the queue may or may not be empty. It must NOT
        ;; look like an empty get — the drain's until-empty reads that as
        ;; "done" and would quit at the first blackhole.
        (do (mark-dead! c)
            (assoc op :type :info
                   :value {:error {:code nil, :text "basic.get timed out"}}))

        (nil? m)
        (assoc op :type :ok
               :value {:queue q, :msg nil, :redelivered false, :depth nil})

        :else
        (let [[pid seq] (amqp/decode-msg (:body m))
              acked     (try (amqp/ack ch (:tag m)) true
                             (catch Exception _
                               (mark-dead! c) false))]
          (assoc op :type :ok
                 :value {:queue        q
                         :msg          [pid seq]
                         :redelivered  (:redelivered m)
                         :depth        (:depth m)
                         :acked        acked}))))
    (catch Exception e
      (mark-dead! c)
      (assoc op :type :info
             :value {:error (amqp/error-info e)}))))

(defn- depth-op!
  "Passive declare to observe remaining queue depth at drain time. This op
  is the drain-completeness witness, so it retries briefly instead of
  collapsing to :info on an end-of-test channel hiccup."
  [c op]
  (loop [attempts 5]
    (let [res (try
                (let [ch (ensure-ch! c false)
                      {:keys [depth]} (amqp/declare-queue-passive! ch (:queue c))]
                  {:ok? true, :depth depth})
                (catch Exception e
                  {:ok? false, :code (amqp/error-code e), :error e}))]
      (cond
        (:ok? res)
        (assoc op :type :ok
               :value {:queue (:queue c), :depth (:depth res)})

        (= 404 (:code res))
        ;; A vanished queue is definitively empty; the 404 killed the channel.
        (assoc op :type :ok :value {:queue (:queue c), :depth -1})

        (pos? (dec attempts))
        (do (mark-dead! c)
            (Thread/sleep 200)
            (recur (dec attempts)))

        :else
        (do (mark-dead! c)
            (assoc op :type :info
                   :value {:error (amqp/error-info (:error res))}))))))

(defn- channel-dead-catch
  "AMQP errors 404/405/406 close the channel but are definitive results.
  Anything else means we don't know."
  [c e]
  (case (amqp/error-code e)
    (404 405 406) ::definitive
    (do (mark-dead! c) ::unknown)))

(defn- declare-op!
  "queue.declare (durable, non-exclusive): creates the register queue if
  absent, redeclares if present. Concurrent declares can lose the
  CreateQueueData race and 404 after the meta write landed; the queue IS
  present then, so we retry briefly and report the definitive result."
  [c op]
  (loop [attempts 4]
    (let [res (try
                (let [ch (ensure-ch! c false)]
                  (amqp/declare-queue! ch (:register c))
                  {:outcome ::ok})
                (catch Exception e
                  {:outcome (if (and (contains? #{404 405 406} (amqp/error-code e))
                                     (pos? (dec attempts)))
                              ::retry
                              ::fail)
                   :error   e}))]
      (cond
        (= ::retry (:outcome res)) (recur (dec attempts))
        (= ::ok (:outcome res))    (assoc op :type :ok :value {:declared true})
        ;; Definitive-but-unlucky: the meta write landed (queue present) even
        ;; if the reply raced away with a 404.
        (contains? #{404 405 406} (amqp/error-code (:error res)))
        (assoc op :type :ok :value {:declared true})
        :else
        (do (mark-dead! c)
            (assoc op :type :info :value {:error (amqp/error-info (:error res))}))))))

(defn- shard-race?
  "True when a 404 came from the *shard* (\"no queue ... on this shard\")
  rather than meta (\"no queue ... in vhost ...\"). A shard 404 during the
  declare/delete race window means the meta write landed but the shard-side
  CreateQueueData hasn't applied yet — the op aborted before taking effect,
  and is safe to retry."
  [e]
  (let [{:keys [code text]} (amqp/error-info e)]
    (and (= 404 code)
         (re-find #"on this shard" (str text)))))

(defn- delete-op!
  "queue.delete. Three distinct 404s can reach the client:

  * meta (\"no queue ... in vhost ...\") - the linearizable DeleteQueue
    itself missed: definitively absent -> :ok {:deleted false}.
  * shard (\"no queue ... on this shard\") - the CreateQueueData race: the
    delete aborted at its Stats precheck, before meta; retry.
  * local-snapshot (\"no queue X\") - this node's stale topology view
    rejected the queue before any raft op; the delete never happened, but
    the queue may well exist. Retry briefly, then give up as :info.

  Anything else is :info."
  [c op]
  (loop [attempts 6]
    (let [res (try
                (let [ch (ensure-ch! c false)]
                  (amqp/delete-queue! ch (:register c))
                  {:outcome ::ok})
                (catch Exception e
                  (let [{:keys [code text]} (amqp/error-info e)
                        text  (str text)]
                    {:outcome (if (not= 404 code)
                                ::unknown
                                (cond
                                  (re-find #"in vhost" text)    ::absent
                                  (re-find #"on this shard" text)
                                  (if (pos? (dec attempts)) ::retry ::did-not-happen)
                                  :else
                                  (if (pos? (dec attempts)) ::retry ::unknown)))
                     :error   e})))]
      (cond
        (= ::retry (:outcome res))
        (do (Thread/sleep 150) (recur (dec attempts)))

        (= ::ok (:outcome res))
        (assoc op :type :ok :value {:deleted true})

        (= ::absent (:outcome res))
        (assoc op :type :ok :value {:deleted false})

        (= ::did-not-happen (:outcome res))
        ;; Knossos requires a :fail completion's :value to equal its
        ;; invocation's (nil here); the detail goes to the log.
        (do (warn "delete lost the CreateQueueData race:" (:error res))
            (assoc op :type :fail :value nil))

        :else
        (do (mark-dead! c)
            (assoc op :type :info
                   :value {:error (amqp/error-info (:error res))}))))))

(defn- exists-op!
  "Passive declare as a read: a meta-level 404 means absent. An existing
  queue's passive declare routes through a shard Stats read, which can hit
  the CreateQueueData race (shard 404): retry, else give up as :info."
  [c op]
  (loop [attempts 4]
    (let [res (try
                (let [ch (ensure-ch! c false)]
                  (amqp/declare-queue-passive! ch (:register c))
                  {:outcome ::ok})
                (catch Exception e
                  {:outcome (cond
                              (and (shard-race? e) (pos? (dec attempts)))
                              ::retry

                              (shard-race? e)
                              ::unknown

                              (= 404 (amqp/error-code e))
                              ::absent

                              :else ::unknown)
                   :error   e}))]
      (cond
        (= ::retry (:outcome res))
        (do (Thread/sleep 100) (recur (dec attempts)))

        (= ::ok (:outcome res))
        (assoc op :type :ok :value {:present true})

        (= ::absent (:outcome res))
        (assoc op :type :ok :value {:present false})

        :else
        (do (mark-dead! c)
            (assoc op :type :info
                   :value {:error (amqp/error-info (:error res))}))))))

;; Client protocol

(defrecord AmqpClient
  [;; configuration
   node           ; assigned by open!
   conn-opts      ; username/password/etc
   queues-by-node ; {node queue} - the queue :get/:depth ops read on each node
   publish        ; {:exchange x :routing-key rk} for :publish ops
   register       ; the topology register queue name (topo test)
   topology       ; {:queues [...] :exchange x} declared in setup!, or nil
   ;; mutable per-worker state
   queue conn ch pending chan-gen uid]
  client/Client
  (open! [c test node]
    (assoc c
           :node     node
           :queue    (get (:queues-by-node c) node)
           :conn     (atom nil)
           :ch       (atom nil)
           :pending  (atom {})
           :chan-gen (atom 0)
           :uid      (inc (rand-int 1000000000))))

  (setup! [c test]
    (setup-topology! c test))

  (invoke! [c test op]
    (case (:f op)
      :publish (publish-op! c op)
      :get     (get-op! c op)
      :depth   (depth-op! c op)
      :declare (declare-op! c op)
      :delete  (delete-op! c op)
      :exists  (exists-op! c op)
      (assoc op :type :info :value {:error "unknown f"})))

  (teardown! [c test]
    ;; Best-effort cleanup of test topology.
    (try
      (let [ch (ensure-ch! c false)]
        (doseq [q (concat (:queues (:topology c))
                          (when (:register c) [(:register c)]))]
          (try (amqp/delete-queue! ch q) (catch Exception _)))
        (when-let [x (:exchange (:topology c))]
          (try (.exchangeDelete ^Channel ch x) (catch Exception _))))
      (catch Exception _)))

  (close! [c test]
    (mark-dead! c)))

(defn client
  "Builds a client factory. Options:

    :conn-opts      username/password/vhost map
    :queues-by-node {node queue} - where :get/:depth ops read
    :publish        {:exchange x :routing-key rk}
    :register       topology register queue (topo test)
    :topology       {:queues [...] :exchange x} to set up"
  [{:keys [conn-opts queues-by-node publish register topology]}]
  (map->AmqpClient {:conn-opts      conn-opts
                    :queues-by-node queues-by-node
                    :publish        publish
                    :register       register
                    :topology       topology}))
